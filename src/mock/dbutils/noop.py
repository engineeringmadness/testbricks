class NoOpModule:
    def __getattr__(self, name):
        return NoOpModule()

    def __call__(self, *args, **kwargs):
        return True

    def help(self, command=None):
        return True
