import os
from pathlib import Path

from alephclient.tasks import crawl_dir
from alephclient.api import AlephAPI


class TestCrawldir(object):
    def setup_method(self):
        self.api = AlephAPI(
            base_url="http://aleph.test/api/2/", api_key="fake_key"
        )

    def test_new_collection(self, mocker):
        mocker.patch.object(self.api, "filter_collections", return_value=[])
        mocker.patch.object(self.api, "create_collection")
        mocker.patch.object(self.api, "update_collection")
        mocker.patch.object(self.api, "ingest_upload")
        crawl_dir(self.api, "alephclient/tests/testdata", "test153", {})
        self.api.create_collection.assert_called_once_with({
            'category': 'other',
            'foreign_id': 'test153',
            'label': 'test153',
            'languages': [],
            'summary': '',
            'casefile': False
        })

    def test_ingest(self, mocker):
        mocker.patch.object(self.api, "filter_collections", return_value=[{
            "id": 2
        }])
        mocker.patch.object(self.api, "ingest_upload")
        mocker.patch.object(self.api, "update_collection")
        crawl_dir(self.api, "alephclient/tests/testdata", "test153", {})
        assert self.api.ingest_upload.call_count == 5
        expected_calls = [
            mocker.call(
                2,
                None,
                metadata={
                    'foreign_id': 'feb',
                    'file_name': 'feb',
                    'languages': []
                }
            ),
            mocker.call(
                2,
                None,
                metadata={
                    'foreign_id': 'jan',
                    'file_name': 'jan',
                    'languages': []
                }
            ),
            mocker.call(
                2,
                Path(os.path.join(os.path.abspath(
                    "alephclient/tests/testdata"), "feb/2.txt"
                )),
                metadata={
                    'parent': {
                        'foreign_id': 'feb'
                    },
                    'foreign_id': 'feb/2.txt',
                    'file_name': '2.txt',
                    'languages': []
                }
            ),
            mocker.call(
                2,
                None,
                metadata={
                    'parent': {
                        'foreign_id': 'jan'
                    },
                    'foreign_id': 'jan/week1',
                    'file_name': 'week1',
                    'languages': []
                }
            ),
            mocker.call(
                2,
                Path(os.path.join(os.path.abspath(
                    "alephclient/tests/testdata"), "jan/week1/1.txt"
                )),
                metadata={
                    'parent': {
                        'foreign_id': 'jan/week1'
                    },
                    'foreign_id': 'jan/week1/1.txt',
                    'file_name': '1.txt',
                    'languages': []
                }
            ),
        ]
        for call in expected_calls:
            assert call in self.api.ingest_upload.mock_calls
