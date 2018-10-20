

class AlephException(Exception):

    def __init__(self, response):
        self.status = response.status_code
        self.response = response
        if response.is_json():
            data = response.json()
            self.status = data.get('status')
            self.message = data.get('message')
        else:
            self.message = response.content

    def __str__(self):
        return self.message or "Unknown Error"
