class NoOpModule:
    def __getattr__(self, name):
        return _noop

    def help(self, command=None):
        return True


def _noop(*args, **kwargs):
    return True
