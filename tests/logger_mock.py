import pytest


class LoggerMock:

    def __init__(self, monkeypatch):
        self._error_occurred = False
        self.raise_exception_on_error = self._raise_exception_on_error
        monkeypatch.setattr(f'backup.Logger.error', self.raise_exception_on_error)

    def _raise_exception_on_error(self, msg):
        self._error_occurred = True
        raise Exception(msg)

    def is_error_occurred(self):
        return self._error_occurred
