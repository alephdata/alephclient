import os
from pathlib import Path

from alephclient.crawldir import crawl_dir
from alephclient.api import AlephAPI, APIResultSet, EntityResultSet


class TestApiSearch:
    fake_url = "http://aleph.test/api/2/"
    fake_query = "fleem"

    def setup_method(self, mocker):
        self.api = AlephAPI(host="http://aleph.test/api/2/", api_key="fake_key")

    def test_search(self, mocker):
        mocker.patch.object(self.api, "_request")
        search_result = self.api.search(self.fake_query)

        assert isinstance(search_result, APIResultSet) == True

    def test_search_url(self, mocker):
        mocker.patch.object(self.api, "_request")
        search_result = self.api.search(self.fake_query)

        assert self.fake_url in search_result.url

    def test_search_query(self, mocker):
        mocker.patch.object(self.api, "_request")
        search_result = self.api.search(self.fake_query)

        assert self.fake_query in search_result.url

    def test_search_schema(self, mocker):
        schema = "Article"
        mocker.patch.object(self.api, "_request")
        search_result = self.api.search(self.fake_query, schema)

        assert "schema=".format(schema) in search_result.url

    def test_search_schemata(self, mocker):
        schemata = "Document"
        mocker.patch.object(self.api, "_request")
        search_result = self.api.search(self.fake_query, None, schemata)

        assert "schemata=".format(schemata) in search_result.url

    def test_search_params(self, mocker):
        params = {"first": "first", "second": "second"}
        mocker.patch.object(self.api, "_request")
        search_result = self.api.search(self.fake_query, params=params)

        assert "first=first" in search_result.url
        assert "second=second" in search_result.url
