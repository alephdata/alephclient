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

    def test_reindex_collection(self, mocker):
        pass

    def test_delete_collection(self, mocker):
        pass

    def test_flush_collection(self, mocker):
        pass

    def test_get_collection_by_foreign_id(self, mocker):
        pass

    def test_load_collection_by_foreign_id(self, mocker):
        pass

    def test_filter_collections(self, mocker):
        pass

    def test_create_collection(self, mocker):
        pass
