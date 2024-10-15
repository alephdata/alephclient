import pytest
from requests import Response
from alephclient.errors import AlephException
from alephclient.api import AlephAPI


@pytest.fixture
def http_error_response():
    response = Response()
    response.status_code = 502
    return response


class TestApiCollection:
    fake_url = "http://aleph.test/api/2/"

    def setup_method(self, mocker):
        self.api = AlephAPI(host=self.fake_url, api_key="fake_key")

    def test_502(self, mocker, http_error_response):
        # Test that the _request method raises AlephException
        # if a 4XX or 5XX HTTP status code is encountered.
        collection_id = "8"

        mocker.patch.object(
            self.api.session, "request",
            return_value=http_error_response,
        )

        with pytest.raises(AlephException):
            self.api.get_collection(collection_id)

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

    def test_touch_collection(self, mocker):
        pass

    def test_get_collection_by_foreign_id(self, mocker):
        pass

    def test_load_collection_by_foreign_id(self, mocker):
        pass

    def test_filter_collections(self, mocker):
        pass

    def test_create_collection(self, mocker):
        pass
