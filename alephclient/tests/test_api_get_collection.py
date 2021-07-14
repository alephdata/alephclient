from alephclient.api import AlephAPI, APIResultSet


class TestApiGetCollection:
    def setup_method(self, mocker):
        self.api = AlephAPI(host="http://aleph.test/api/2/", api_key="fake_key")

    def test_get_collection(self, mocker):
        mocker.patch.object(self.api, "_request")
        self.api.get_collection("8")
        self.api._request.assert_called_with(
            "GET", "http://aleph.test/api/2/collections/8"
        )
