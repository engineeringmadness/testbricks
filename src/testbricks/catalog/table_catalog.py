"""CSV + temp-view registry for SparkProxy tables."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Mapping, Optional

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.utils import AnalysisException

from .errors import SchemaMismatchError
from .identifier import TableIdentifier

_ERROR_MODES = frozenset({"error", "errorifexists"})
_APPEND_MODES = frozenset({"append"})
_OVERWRITE_MODES = frozenset({"overwrite"})
_IGNORE_MODES = frozenset({"ignore"})

_DEFAULT_READ_OPTIONS = {"header": "true", "inferSchema": "true"}
_CSV_OPTION_KEYS = frozenset(
    {
        "delimiter",
        "sep",
        "quote",
        "escape",
        "nullvalue",
        "dateformat",
        "timestampformat",
        "header",
    }
)


class TableCatalog:
    """Maps ``schema.table`` identifiers to local CSV files and Spark temp views."""

    def __init__(self, spark_session: SparkSession, base_path: str):
        self._spark = spark_session
        self._base_path = base_path
        self._root = Path(base_path)
        self._csv_options: dict[str, dict[str, str]] = {}

    @property
    def base_path(self) -> str:
        return self._base_path

    def path_for(self, ident: TableIdentifier) -> str:
        return str(self._root / ident.relative_csv_path)

    def options_path_for(self, ident: TableIdentifier) -> str:
        return str(self._root / ident.schema / f"{ident.table}.options.json")

    def full_path(self, relative_path: str) -> str:
        return str(self._root / relative_path)

    def ensure_schema_dir(self, ident: TableIdentifier) -> str:
        schema_path = self._root / ident.schema
        schema_path.mkdir(parents=True, exist_ok=True)
        return str(schema_path)

    def exists(self, ident: TableIdentifier) -> bool:
        return os.path.exists(self.path_for(ident))

    def csv_options_for(self, ident: TableIdentifier) -> dict[str, str]:
        key = str(ident)
        if key not in self._csv_options:
            self._csv_options[key] = _load_options_file(self.options_path_for(ident))
        return dict(self._csv_options[key])

    def iter_schema_names(self) -> list[str]:
        if not self._root.exists():
            return []
        return sorted(path.name for path in self._root.iterdir() if path.is_dir())

    def iter_identifiers(self) -> list[TableIdentifier]:
        idents: list[TableIdentifier] = []
        for schema in self.iter_schema_names():
            for csv_path in sorted((self._root / schema).glob("*.csv")):
                idents.append(TableIdentifier(schema=schema, table=csv_path.stem))
        return idents

    def load_all(self) -> None:
        for ident in self.iter_identifiers():
            self.read_csv(ident).createOrReplaceTempView(ident.view_name)

    def read_csv(
        self,
        ident: TableIdentifier,
        options: Optional[Mapping[str, str]] = None,
    ) -> DataFrame:
        merged = {
            **_DEFAULT_READ_OPTIONS,
            **self.csv_options_for(ident),
            **dict(options or {}),
        }
        reader = self._spark.read
        for key, value in merged.items():
            reader = reader.option(key, value)
        return reader.csv(self.path_for(ident))

    def save_dataframe(
        self,
        ident: TableIdentifier,
        dataframe: DataFrame,
        mode: Optional[str] = None,
        header: bool = True,
        replace_where: Optional[str] = None,
        csv_options: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.ensure_schema_dir(ident)
        csv_path = self.path_for(ident)
        exists = os.path.exists(csv_path)
        save_mode = _normalize_save_mode(mode)

        if exists and save_mode in _ERROR_MODES:
            raise AnalysisException(
                f"[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view "
                f"{ident} because it already exists."
            )
        if exists and save_mode in _IGNORE_MODES:
            return

        stored = self.csv_options_for(ident) if exists else {}
        incoming = _normalize_csv_options(csv_options)
        if save_mode in _APPEND_MODES and exists:
            effective_options = {**stored, **incoming}
        else:
            effective_options = {**incoming}
        if header:
            effective_options.setdefault("header", "true")
        else:
            effective_options["header"] = "false"

        new_pdf = dataframe.toPandas()
        new_pdf = _format_temporal_columns(new_pdf, effective_options)

        if replace_where:
            if save_mode != "overwrite":
                raise ValueError(
                    "option('replaceWhere') requires mode('overwrite'); "
                    f"got mode '{mode}'."
                )
            if exists:
                existing_pdf = _pandas_read_csv(csv_path, stored or effective_options)
                remaining = _apply_replace_where(existing_pdf, replace_where)
                aligned = _align_columns_for_concat(remaining, new_pdf)
                new_pdf = pd.concat(aligned, ignore_index=True)
        elif save_mode in _APPEND_MODES and exists:
            existing_pdf = _pandas_read_csv(csv_path, stored or effective_options)
            if set(existing_pdf.columns) != set(new_pdf.columns):
                raise SchemaMismatchError(
                    f"Cannot append to '{ident}': schema mismatch. "
                    f"Existing columns={list(existing_pdf.columns)}, "
                    f"new columns={list(new_pdf.columns)}"
                )
            new_pdf = pd.concat(
                [existing_pdf, new_pdf[existing_pdf.columns]],
                ignore_index=True,
            )

        self._write_csv_atomic(new_pdf, csv_path, header=header, options=effective_options)
        self._persist_csv_options(ident, effective_options)
        self._spark.createDataFrame(new_pdf).createOrReplaceTempView(ident.view_name)

    def _persist_csv_options(self, ident: TableIdentifier, options: Mapping[str, str]) -> None:
        payload = {key: str(value) for key, value in options.items()}
        self._csv_options[str(ident)] = dict(payload)
        options_path = self.options_path_for(ident)
        with open(options_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)

    @staticmethod
    def _write_csv_atomic(
        pandas_df: pd.DataFrame,
        csv_path: str,
        header: bool = True,
        options: Optional[Mapping[str, str]] = None,
    ) -> None:
        directory = os.path.dirname(csv_path)
        fd, temp_path = tempfile.mkstemp(suffix=".csv", dir=directory)
        os.close(fd)
        try:
            pandas_df.to_csv(temp_path, **_pandas_write_kwargs(options, header=header))
            os.replace(temp_path, csv_path)
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise


def _normalize_save_mode(mode: Optional[str]) -> str:
    """Map Spark save-mode aliases; default (None) overwrites, matching SparkProxy today."""
    if mode is None:
        return "overwrite"
    normalized = str(mode).strip().lower()
    if normalized in _ERROR_MODES:
        return "error"
    if normalized in _APPEND_MODES:
        return "append"
    if normalized in _OVERWRITE_MODES:
        return "overwrite"
    if normalized in _IGNORE_MODES:
        return "ignore"
    raise ValueError(
        f"Unknown save mode '{mode}'. Expected overwrite, append, ignore, "
        "error, or errorIfExists."
    )


def _normalize_csv_options(options: Optional[Mapping[str, str]]) -> dict[str, str]:
    if not options:
        return {}
    aliases = {
        "sep": "delimiter",
        "delimiter": "delimiter",
        "quote": "quote",
        "escape": "escape",
        "nullvalue": "nullValue",
        "dateformat": "dateFormat",
        "timestampformat": "timestampFormat",
        "header": "header",
    }
    normalized: dict[str, str] = {}
    for key, value in options.items():
        canonical = aliases.get(key.lower())
        if canonical is None:
            continue
        normalized[canonical] = str(value)
    return normalized


def _option_lookup(options: Optional[Mapping[str, str]], *names: str) -> Optional[str]:
    if not options:
        return None
    lowered = {key.lower(): value for key, value in options.items()}
    for name in names:
        if name.lower() in lowered:
            return str(lowered[name.lower()])
    return None


def _pandas_write_kwargs(options: Optional[Mapping[str, str]], header: bool) -> dict:
    kwargs: dict = {"index": False, "header": header}
    delimiter = _option_lookup(options, "delimiter", "sep")
    if delimiter:
        kwargs["sep"] = delimiter
    quote = _option_lookup(options, "quote")
    if quote:
        kwargs["quotechar"] = quote
    escape = _option_lookup(options, "escape")
    if escape:
        kwargs["escapechar"] = escape
        kwargs["doublequote"] = False
    null_value = _option_lookup(options, "nullValue")
    if null_value is not None:
        kwargs["na_rep"] = null_value
    date_format = _option_lookup(options, "dateFormat", "timestampFormat")
    if date_format:
        kwargs["date_format"] = java_date_format_to_strftime(date_format)
    return kwargs


def _pandas_read_csv(csv_path: str, options: Optional[Mapping[str, str]]) -> pd.DataFrame:
    kwargs: dict = {}
    delimiter = _option_lookup(options, "delimiter", "sep")
    if delimiter:
        kwargs["sep"] = delimiter
    quote = _option_lookup(options, "quote")
    if quote:
        kwargs["quotechar"] = quote
    escape = _option_lookup(options, "escape")
    if escape:
        kwargs["escapechar"] = escape
    null_value = _option_lookup(options, "nullValue")
    if null_value is not None:
        kwargs["na_values"] = [null_value]
    header = _option_lookup(options, "header")
    if header and header.lower() == "false":
        kwargs["header"] = None
    return pd.read_csv(csv_path, **kwargs)


def _load_options_file(path: str) -> dict[str, str]:
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        return {}
    return {str(key): str(value) for key, value in payload.items()}


def java_date_format_to_strftime(fmt: str) -> str:
    result = fmt
    for java_token, python_token in (
        ("yyyy", "%Y"),
        ("SSS", "%f"),
        ("yy", "%y"),
        ("MM", "%m"),
        ("dd", "%d"),
        ("HH", "%H"),
        ("mm", "%M"),
        ("ss", "%S"),
    ):
        result = result.replace(java_token, python_token)
    return result


def _format_value(value, strftime_fmt: str):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    if hasattr(value, "strftime"):
        return value.strftime(strftime_fmt)
    return value


def _format_temporal_columns(pdf: pd.DataFrame, options: Mapping[str, str]) -> pd.DataFrame:
    date_fmt = _option_lookup(options, "dateFormat")
    ts_fmt = _option_lookup(options, "timestampFormat")
    if not date_fmt and not ts_fmt:
        return pdf
    formatted = pdf.copy()
    for column in formatted.columns:
        series = formatted[column]
        if pd.api.types.is_datetime64_any_dtype(series):
            has_time = bool((series.dt.hour.fillna(0) != 0).any() or (series.dt.minute.fillna(0) != 0).any())
            chosen = ts_fmt if (has_time and ts_fmt) else (date_fmt or ts_fmt)
            formatted[column] = series.dt.strftime(java_date_format_to_strftime(chosen))
            continue
        sample = series.dropna()
        if sample.empty or not hasattr(sample.iloc[0], "strftime"):
            continue
        chosen = ts_fmt if ts_fmt and hasattr(sample.iloc[0], "hour") else (date_fmt or ts_fmt)
        if not chosen:
            continue
        strftime_fmt = java_date_format_to_strftime(chosen)
        formatted[column] = series.map(lambda value: _format_value(value, strftime_fmt))
    return formatted


def _spark_predicate_to_pandas_query(predicate: str) -> str:
    query = predicate.strip()
    query = query.replace("`", "")
    query = re.sub(r"\bAND\b", "and", query, flags=re.IGNORECASE)
    query = re.sub(r"\bOR\b", "or", query, flags=re.IGNORECASE)
    query = re.sub(r"\bNOT\b", "not", query, flags=re.IGNORECASE)
    query = re.sub(r"(?<![<>!=])=(?!=)", "==", query)
    return query


def _apply_replace_where(existing_pdf: pd.DataFrame, predicate: str) -> pd.DataFrame:
    query = _spark_predicate_to_pandas_query(predicate)
    try:
        matching = existing_pdf.query(query)
    except Exception as exc:
        raise ValueError(
            f"Unparsable replaceWhere predicate: {predicate!r}"
        ) from exc
    return existing_pdf.drop(matching.index)


def _align_columns_for_concat(left: pd.DataFrame, right: pd.DataFrame) -> list[pd.DataFrame]:
    columns = list(dict.fromkeys(list(left.columns) + list(right.columns)))
    return [left.reindex(columns=columns), right.reindex(columns=columns)]
