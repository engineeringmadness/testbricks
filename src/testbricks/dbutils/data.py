class DataMock:
    def summarize(self, df, precise=False):
        """Print `df.describe()` for Spark or pandas frames."""
        _ = precise
        described = df.describe()
        show = getattr(described, "show", None)
        if callable(show):
            show()
        else:
            print(described)
        return None
