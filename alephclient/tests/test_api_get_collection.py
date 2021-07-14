from alephclient.api import AlephAPI, APIResultSet


class TestApiGetCollection:
    fake_url = "http://aleph.test/api/2/"

    def setup_method(self, mocker):
        self.api = AlephAPI(host=self.fake_url, api_key="fake_key")

    def test_get_collection(self, mocker):
        mocker.patch.object(self.api, "_request")
        self.api.get_collection("8")
        self.api._request.assert_called_with(
            "GET", "{}collections/8".format(self.fake_url)
        )
