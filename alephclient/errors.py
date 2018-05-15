

class AlephException(Exception):
    def __init__(self, status, message=None, response=None):
        self.status = status
        self.response = response
        self.message = message

    def __str__(self):
        return "%s (%s)" % (self.status, self.message or "Unknown Error")

    def __repr__(self):
        return "%s(status=%s)" % (self.__class__.__name__, self.status)
