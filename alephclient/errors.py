

class AlephException(Exception):

    def __init__(self, response):
        self.status = response.status_code
        self.response = response
        try:
            data = response.json()
            self.status = data.get('status')
            self.message = data.get('message')
        except Exception:
            self.message = response.text

    def __str__(self):
        return self.message
