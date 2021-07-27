
class LoggerMock:

    def __init__(self, monkeypatch):
        self.raise_exception_on_error = self._raise_exception_on_error
        monkeypatch.setattr(f'backup.Logger.error', self.raise_exception_on_error)

    def _raise_exception_on_error(self, msg):
        raise Exception(msg)

