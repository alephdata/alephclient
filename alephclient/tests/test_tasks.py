import os

from ..tasks import crawl_dir
from ..api import AlephAPI


class TestCrawldir(object):
    def setup_method(self):
        self.api = AlephAPI(
            base_url="http://aleph.test/api/2/", api_key="fake_key"
        )

    def test_new_collection(self, mocker):
        mocker.patch.object(self.api, "filter_collections", return_value=[])
        mocker.patch.object(self.api, "create_collection")
        mocker.patch.object(self.api, "ingest_upload")
        crawl_dir(self.api, "alephclient/tests/testdata", "test153")
        self.api.create_collection.assert_called_once_with(
            data={'category': 'other',
                  'foreign_id': 'test153',
                  'label': 'testdata',
                  'managed': True}
        )

    def test_ingest(self, mocker):
        mocker.patch.object(self.api, "filter_collections", return_value=[{
            "id": 2
        }])
        mocker.patch.object(self.api, "ingest_upload")
        crawl_dir(self.api, "alephclient/tests/testdata", "test153")
        assert self.api.ingest_upload.call_count == 5
        expected_calls = [
            mocker.call(
                2,
                metadata={
                    'parent': {
                        'foreign_id': 'test153'
                    },
                    'foreign_id': 'test153/feb',
                    'file_name': 'feb'
                }
            ),
            mocker.call(
                2,
                metadata={
                    'parent': {
                        'foreign_id': 'test153'
                    },
                    'foreign_id': 'test153/jan',
                    'file_name': 'jan'
                }
            ),
            mocker.call(
                2,
                os.path.join(os.path.abspath(
                    "alephclient/tests/testdata"), "feb/2.txt"
                ),
                {
                    'parent': {
                        'foreign_id': 'test153/feb'
                    },
                    'foreign_id': 'test153/feb/2.txt',
                    'file_name': '2.txt'
                }
            ),
            mocker.call(
                2,
                metadata={
                    'parent': {
                        'foreign_id': 'test153/jan'
                    },
                    'foreign_id': 'test153/jan/week1',
                    'file_name': 'week1'
                }
            ),
            mocker.call(
                2,
                os.path.join(os.path.abspath(
                    "alephclient/tests/testdata"), "jan/week1/1.txt"
                ),
                {
                    'parent': {
                        'foreign_id': 'test153/jan/week1'
                    },
                    'foreign_id': 'test153/jan/week1/1.txt',
                    'file_name': '1.txt'
                }
            ),
        ]
        assert self.api.ingest_upload.mock_calls == expected_calls
