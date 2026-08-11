"""SQL entrypoint for SparkMock: safe table-name rewrite + subset DML."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .spark_mock import SparkMock

_IDENT = r"[A-Za-z_][\w]*"
_TWO_PART = rf"({_IDENT})\.({_IDENT})"

# Table-introducing keywords. USING only when not USING(...).
_TABLE_INTRO_RE = re.compile(
    rf"(?i)\b(FROM|JOIN|INTO|UPDATE|TABLE|MERGE\s+INTO|USING)\s+(?!SELECT\b)({_IDENT})\.({_IDENT})\b"
)

_DELETE_RE = re.compile(
    rf"(?is)^\s*DELETE\s+FROM\s+({_IDENT})\.({_IDENT})(?:\s+WHERE\s+(.+))?$"
)
_UPDATE_RE = re.compile(
    rf"(?is)^\s*UPDATE\s+({_IDENT})\.({_IDENT})\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$"
)
_INSERT_SELECT_RE = re.compile(
    rf"(?is)^\s*INSERT\s+INTO\s+({_IDENT})\.({_IDENT})\s+(SELECT\b.+)$"
)
_CTAS_RE = re.compile(
    rf"(?is)^\s*CREATE\s+(OR\s+REPLACE\s+)?TABLE\s+({_IDENT})\.({_IDENT})"
    rf"(?:\s+USING\s+\w+)?\s+AS\s+(SELECT\b.+)$"
)
_MERGE_RE = re.compile(
    rf"(?is)^\s*MERGE\s+INTO\s+({_IDENT})\.({_IDENT})\s+(\w+)\s+"
    rf"USING\s+({_IDENT})\.({_IDENT})\s+(\w+)\s+"
    rf"ON\s+(.+?)\s+"
    rf"WHEN\s+MATCHED\s+THEN\s+(.+?)\s+"
    rf"WHEN\s+NOT\s+MATCHED\s+THEN\s+(.+?)\s*$"
)


def _strip_leading_comments(sql: str) -> str:
    text = sql.strip()
    while True:
        if text.startswith("--"):
            nl = text.find("\n")
            text = text[nl + 1 :].lstrip() if nl >= 0 else ""
            continue
        if text.startswith("/*"):
            end = text.find("*/")
            text = text[end + 2 :].lstrip() if end >= 0 else ""
            continue
        break
    return text


def rewrite_two_part_identifiers(sql: str) -> str:
    """Rewrite schema.table references after table-introducing keywords only."""

    def repl(match: re.Match) -> str:
        keyword = match.group(1)
        schema = match.group(2)
        table = match.group(3)
        # Preserve JOIN ... USING (col) — group 1 would be USING but pattern
        # requires ident.ident after USING, so USING (x) is not matched.
        return f"{keyword} {schema}_{table}"

    return _TABLE_INTRO_RE.sub(repl, sql)


def _classify(sql: str) -> str:
    head = _strip_leading_comments(sql).lstrip().upper()
    if head.startswith("MERGE"):
        return "MERGE"
    if head.startswith("DELETE"):
        return "DELETE"
    if head.startswith("UPDATE"):
        return "UPDATE"
    if head.startswith("INSERT"):
        return "INSERT"
    if head.startswith("CREATE"):
        return "CREATE"
    if head.startswith("SELECT") or head.startswith("WITH"):
        return "SELECT"
    return "OTHER"


class SqlGateway:
    def __init__(self, spark_mock: "SparkMock"):
        self._spark = spark_mock
        self._session = spark_mock._spark_session
        self._catalog = spark_mock._catalog

    def execute(self, query: str):
        kind = _classify(query)
        if kind == "SELECT":
            return self._execute_select(query)
        if kind == "DELETE":
            self._execute_delete(query)
            return None
        if kind == "UPDATE":
            self._execute_update(query)
            return None
        if kind == "INSERT":
            self._execute_insert(query)
            return None
        if kind == "CREATE":
            self._execute_ctas(query)
            return None
        if kind == "MERGE":
            self._execute_merge(query)
            return None

        # Fallback: try SELECT-style rewrite for uncommon statements.
        rewritten = rewrite_two_part_identifiers(query)
        df = self._session.sql(rewritten)
        from .data_frame_wrapper import DataFrameWrapper

        return DataFrameWrapper(self._spark, df)

    def _execute_select(self, query: str):
        rewritten = rewrite_two_part_identifiers(query)
        df = self._session.sql(rewritten)
        from .data_frame_wrapper import DataFrameWrapper

        return DataFrameWrapper(self._spark, df)

    def _execute_delete(self, query: str) -> None:
        text = _strip_leading_comments(query).rstrip(";").strip()
        match = _DELETE_RE.match(text)
        if not match:
            raise NotImplementedError(
                "Unsupported DELETE syntax. Expected: DELETE FROM schema.table [WHERE ...]"
            )
        if re.search(r"(?i)\bJOIN\b", text):
            raise NotImplementedError("DELETE with JOIN is not supported")

        schema, table, where = match.group(1), match.group(2), match.group(3)
        name = f"{schema}.{table}"
        df = self._catalog.read_table(name)
        if where and where.strip():
            remaining = df.filter(f"NOT ({where.strip()})")
        else:
            remaining = df.limit(0)
        self._catalog.write_table(name, remaining, mode="overwrite")

    def _execute_update(self, query: str) -> None:
        text = _strip_leading_comments(query).rstrip(";").strip()
        match = _UPDATE_RE.match(text)
        if not match:
            raise NotImplementedError(
                "Unsupported UPDATE syntax. Expected: UPDATE schema.table SET ... [WHERE ...]"
            )
        if re.search(r"(?i)\bJOIN\b", text):
            raise NotImplementedError("UPDATE with JOIN is not supported")

        schema, table = match.group(1), match.group(2)
        set_clause, where = match.group(3), match.group(4)
        name = f"{schema}.{table}"
        df = self._catalog.read_table(name)
        view = self._catalog.view_name(schema, table)

        assignments = self._parse_assignments(set_clause)
        where_sql = where.strip() if where and where.strip() else "TRUE"
        select_parts = []
        for col in df.columns:
            if col in assignments:
                select_parts.append(
                    f"CASE WHEN {where_sql} THEN {assignments[col]} ELSE `{col}` END AS `{col}`"
                )
            else:
                select_parts.append(f"`{col}`")
        updated = self._session.sql(
            f"SELECT {', '.join(select_parts)} FROM {view}"
        )
        self._catalog.write_table(name, updated, mode="overwrite")

    def _parse_assignments(self, set_clause: str) -> dict[str, str]:
        assignments: dict[str, str] = {}
        # Split on commas not inside quotes.
        parts: list[str] = []
        buf: list[str] = []
        in_single = False
        in_double = False
        for ch in set_clause:
            if ch == "'" and not in_double:
                in_single = not in_single
            elif ch == '"' and not in_single:
                in_double = not in_double
            if ch == "," and not in_single and not in_double:
                parts.append("".join(buf).strip())
                buf = []
                continue
            buf.append(ch)
        if buf:
            parts.append("".join(buf).strip())

        for part in parts:
            if "=" not in part:
                raise NotImplementedError(f"Unsupported SET assignment: {part}")
            left, right = part.split("=", 1)
            col = left.strip()
            # Allow optional alias prefix: t.col -> col
            if "." in col:
                col = col.split(".")[-1]
            assignments[col] = right.strip()
        return assignments

    def _execute_insert(self, query: str) -> None:
        text = _strip_leading_comments(query).rstrip(";").strip()
        match = _INSERT_SELECT_RE.match(text)
        if not match:
            raise NotImplementedError(
                "Unsupported INSERT syntax. Expected: INSERT INTO schema.table SELECT ..."
            )
        schema, table, select_sql = match.group(1), match.group(2), match.group(3)
        name = f"{schema}.{table}"
        rewritten_select = rewrite_two_part_identifiers(select_sql.strip())
        df = self._session.sql(rewritten_select)
        self._catalog.write_table(name, df, mode="append")

    def _execute_ctas(self, query: str) -> None:
        text = _strip_leading_comments(query).rstrip(";").strip()
        match = _CTAS_RE.match(text)
        if not match:
            raise NotImplementedError(
                "Unsupported CREATE TABLE syntax. Expected: "
                "CREATE [OR REPLACE] TABLE schema.table AS SELECT ..."
            )
        replace, schema, table, select_sql = (
            match.group(1),
            match.group(2),
            match.group(3),
            match.group(4),
        )
        name = f"{schema}.{table}"
        if not replace and self._catalog.table_exists(name):
            raise FileExistsError(f"Table already exists: {name}")
        rewritten_select = rewrite_two_part_identifiers(select_sql.strip())
        df = self._session.sql(rewritten_select)
        self._catalog.write_table(name, df, mode="overwrite")

    def _execute_merge(self, query: str) -> None:
        text = _strip_leading_comments(query).rstrip(";").strip()
        match = _MERGE_RE.match(text)
        if not match:
            raise NotImplementedError(
                "Unsupported MERGE syntax. Expected subset: "
                "MERGE INTO t USING s ON ... WHEN MATCHED THEN UPDATE SET ... "
                "WHEN NOT MATCHED THEN INSERT (...) VALUES (...)"
            )

        (
            t_schema,
            t_table,
            t_alias,
            s_schema,
            s_table,
            s_alias,
            on_clause,
            matched_action,
            not_matched_action,
        ) = match.groups()

        matched_action = matched_action.strip()
        not_matched_action = not_matched_action.strip()

        if re.match(r"(?i)^DELETE\b", matched_action):
            raise NotImplementedError("WHEN MATCHED THEN DELETE is not supported")
        if not re.match(r"(?i)^UPDATE\s+SET\b", matched_action):
            raise NotImplementedError(
                "Only WHEN MATCHED THEN UPDATE SET ... is supported"
            )
        insert_match = re.match(
            rf"(?is)^INSERT\s*\((.+?)\)\s*VALUES\s*\((.+)\)$", not_matched_action
        )
        if not insert_match:
            raise NotImplementedError(
                "Only WHEN NOT MATCHED THEN INSERT (cols) VALUES (exprs) is supported"
            )

        target_name = f"{t_schema}.{t_table}"
        source_name = f"{s_schema}.{s_table}"
        target_df = self._catalog.read_table(target_name)
        source_df = self._catalog.read_table(source_name)
        t_view = self._catalog.view_name(t_schema, t_table)
        s_view = self._catalog.view_name(s_schema, s_table)

        set_sql = re.sub(r"(?i)^UPDATE\s+SET\s+", "", matched_action).strip()
        assignments = self._parse_assignments(set_sql)
        insert_cols = [c.strip() for c in insert_match.group(1).split(",")]
        insert_vals = [v.strip() for v in self._split_csv_respecting_quotes(insert_match.group(2))]

        # Unmatched target rows (keep as-is)
        keep_sql = (
            f"SELECT {t_alias}.* FROM {t_view} {t_alias} "
            f"LEFT ANTI JOIN {s_view} {s_alias} ON {on_clause}"
        )
        keep_df = self._session.sql(keep_sql)

        # Matched rows with updates
        select_parts = []
        for col in target_df.columns:
            if col in assignments:
                expr = assignments[col]
                select_parts.append(f"{expr} AS `{col}`")
            else:
                select_parts.append(f"{t_alias}.`{col}` AS `{col}`")
        matched_sql = (
            f"SELECT {', '.join(select_parts)} FROM {t_view} {t_alias} "
            f"INNER JOIN {s_view} {s_alias} ON {on_clause}"
        )
        matched_df = self._session.sql(matched_sql)

        # Not matched source -> insert
        insert_select = ", ".join(
            f"{val} AS `{col}`" for col, val in zip(insert_cols, insert_vals)
        )
        insert_sql = (
            f"SELECT {insert_select} FROM {s_view} {s_alias} "
            f"LEFT ANTI JOIN {t_view} {t_alias} ON {on_clause}"
        )
        insert_df = self._session.sql(insert_sql)

        # Align insert columns to target schema order
        insert_df = insert_df.select(*target_df.columns)
        matched_df = matched_df.select(*target_df.columns)
        keep_df = keep_df.select(*target_df.columns)

        result = keep_df.unionByName(matched_df).unionByName(insert_df)
        self._catalog.write_table(target_name, result, mode="overwrite")

    @staticmethod
    def _split_csv_respecting_quotes(text: str) -> list[str]:
        parts: list[str] = []
        buf: list[str] = []
        in_single = False
        depth = 0
        for ch in text:
            if ch == "'" and depth == 0:
                in_single = not in_single
            if not in_single:
                if ch == "(":
                    depth += 1
                elif ch == ")" and depth:
                    depth -= 1
                elif ch == "," and depth == 0:
                    parts.append("".join(buf).strip())
                    buf = []
                    continue
            buf.append(ch)
        if buf:
            parts.append("".join(buf).strip())
        return parts
