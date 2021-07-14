from alephclient.api import AlephAPI, APIResultSet


class TestApiCollection:
    fake_url = "http://aleph.test/api/2/"

    def setup_method(self, mocker):
        self.api = AlephAPI(host=self.fake_url, api_key="fake_key")

    def test_get_collection(self, mocker):
        collection_id = "8"
        mocker.patch.object(self.api, "_request")
        self.api.get_collection(collection_id)
        self.api._request.assert_called_with(
            "GET", "{}collections/{}".format(self.fake_url, collection_id)
        )

    def test_reingest_collection(self, mocker):
        pass
