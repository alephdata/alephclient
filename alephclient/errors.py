from requests import ConnectionError, Timeout


class AlephException(Exception):
    def __init__(self, exc):
        self.exc = exc
        self.response = None
        self.status = None
        self.transient = isinstance(exc, (ConnectionError, Timeout))
        self.message = str(exc)
        if hasattr(exc, "response") and exc.response is not None:
            self.response = exc.response
            self.status = exc.response.status_code
            self.transient = exc.response.status_code >= 500
            try:
                data = exc.response.json()
                self.message = data.get("message")
            except Exception:
                self.message = exc.response.text

    def __str__(self):
        return self.message
