class LibraryMock:
    def restartPython(self):
        """Return True without restarting; the local process is intentionally not restarted."""
        return True
