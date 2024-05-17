from alephclient.api import AlephAPI
from alephclient.load_catalog import load_catalog


class TestLoadCatalog:
    fake_url = "http://aleph.test/api/2/"
    catalog_url = "https://data.opensanctions.org/datasets/latest/index.json"

    def setup_method(self, mocker):
        self.api = AlephAPI(host=self.fake_url, api_key="fake_key")

    def test_load_catalog(self, mocker):
        mocker.patch.object(self.api, "_request")
        for collection_id, loader in load_catalog(self.api, self.catalog_url):
            self.api._request.assert_called_with(
                "GET", "{}collections/{}".format(self.fake_url, collection_id)
            )
            for data in loader:
                assert isinstance(data, dict)
                break
            break
