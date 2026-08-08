class NotebookExit(BaseException):
    def __init__(self, value: str):
        self.value = value
        super().__init__(value)
