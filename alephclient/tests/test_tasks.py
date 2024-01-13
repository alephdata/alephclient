import os
from pathlib import Path

from alephclient.crawldir import crawl_dir
from alephclient.api import AlephAPI

class TestTasks(object):
    def setup_method(self):
        self.api = AlephAPI(host="http://aleph.test/api/2/", api_key="fake_key")

    def test_new_collection(self, mocker):
        mocker.patch.object(self.api, "filter_collections", return_value=[])
        mocker.patch.object(self.api, "create_collection")
        mocker.patch.object(self.api, "update_collection")
        mocker.patch.object(self.api, "ingest_upload")
        crawl_dir(self.api, "alephclient/tests/testdata", "test153", {}, True, True)
        self.api.create_collection.assert_called_once_with(
            {
                "category": "other",
                "foreign_id": "test153",
                "label": "test153",
                "languages": [],
                "summary": "",
                "casefile": False,
            }
        )

    def test_write_entity(self, mocker):
        mocker.patch.object(self.api, "write_entity", return_value={"id": 24})
        collection_id = 8
        entity = {
            "id": 24,
            "schema": "Article",
            "properties": {
                "title": "",
                "author": "",
                "publishedAt": "",
                "bodyText": "",
            },
        }

        res = self.api.write_entity(collection_id, entity)
        assert res["id"] == 24


    def test_delete_entity(self, mocker):
        mocker.patch.object(self.api, "write_entity", return_value={"id": 24})
        collection_id = 8
        entity = {
            "id": 24,
            "schema": "Article",
            "properties": {
                "title": "",
                "author": "",
                "publishedAt": "",
                "bodyText": "",
            },
        }

        res = self.api.write_entity(collection_id, entity)
        assert res['id'] == 24
        dres = self.api.delete_entity(eid)
        assert dres  == {}


    def test_ingest(self, mocker):
        mocker.patch.object(self.api, "ingest_upload", return_value={"id": 42})
        mocker.patch.object(
            self.api, "load_collection_by_foreign_id", return_value={"id": 2}
        )
        mocker.patch.object(self.api, "update_collection")
        crawl_dir(self.api, "alephclient/tests/testdata", "test153", {}, True, True)
        base_path = os.path.abspath("alephclient/tests/testdata")
        assert self.api.ingest_upload.call_count == 6
        expected_calls = [
            mocker.call(
                2,
                Path(os.path.join(base_path, "feb")),
                metadata={"foreign_id": "feb", "file_name": "feb"},
                index=True,
            ),
            mocker.call(
                2,
                Path(os.path.join(base_path, "jan")),
                metadata={"foreign_id": "jan", "file_name": "jan"},
                index=True,
            ),
            mocker.call(
                2,
                Path(os.path.join(base_path, "feb/2.txt")),
                metadata={
                    "parent_id": 42,
                    "foreign_id": "feb/2.txt",
                    "file_name": "2.txt",
                },
                index=True,
            ),
            mocker.call(
                2,
                Path(os.path.join(base_path, "jan/week1")),
                metadata={
                    "parent_id": 42,
                    "foreign_id": "jan/week1",
                    "file_name": "week1",
                },
                index=True,
            ),
            mocker.call(
                2,
                Path(os.path.join(base_path, "jan/week1/1.txt")),
                metadata={
                    "parent_id": 42,
                    "foreign_id": "jan/week1/1.txt",
                    "file_name": "1.txt",
                },
                index=True,
            ),
        ]
        for call in expected_calls:
            assert call in self.api.ingest_upload.mock_calls
