from unittest.mock import MagicMock

from alephclient.api import AlephAPI
from alephclient.exportdir import list_exports, format_exports_table, download_export


FAKE_EXPORT = {
    "id": "123",
    "label": "My Export",
    "status": "complete",
    "file_name": "export.zip",
    "links": {"download": "http://aleph.test/api/2/archive?token=abc"},
}


class TestListExports:
    fake_url = "http://aleph.test/api/2/"

    def setup_method(self):
        self.api = AlephAPI(host=self.fake_url, api_key="fake_key")

    def test_single_page(self, mocker):
        exports = [{"id": "1", "label": "Export 1"}]
        mocker.patch.object(
            self.api,
            "_request",
            return_value={"results": exports, "next": None},
        )
        result = list_exports(self.api)
        assert result == exports

    def test_pagination(self, mocker):
        page1 = {"results": [{"id": "1"}], "next": self.fake_url + "exports?page=2"}
        page2 = {"results": [{"id": "2"}], "next": None}
        mocker.patch.object(
            self.api,
            "_request",
            side_effect=[page1, page2],
        )
        result = list_exports(self.api)
        assert len(result) == 2
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"

    def test_empty(self, mocker):
        mocker.patch.object(
            self.api,
            "_request",
            return_value={"results": [], "next": None},
        )
        result = list_exports(self.api)
        assert result == []


class TestFormatExportsTable:
    def test_empty_list(self):
        assert format_exports_table([]) == "No exports found."

    def test_with_data(self):
        exports = [
            {
                "id": "abc",
                "label": "My Export",
                "status": "completed",
                "created_at": "2025-01-01",
                "content_hash": "sha1:deadbeef",
            }
        ]
        table = format_exports_table(exports)
        lines = table.split("\n")
        assert len(lines) == 3
        assert "ID" in lines[0]
        assert "Label" in lines[0]
        assert "Status" in lines[0]
        assert "abc" in lines[2]
        assert "My Export" in lines[2]
        assert "completed" in lines[2]


class TestDownloadExport:
    fake_url = "http://aleph.test/api/2/"

    def setup_method(self):
        self.api = AlephAPI(host=self.fake_url, api_key="fake_key")

    def _mock_download(self, mocker, content=b"file content"):
        mock_response = MagicMock()
        mock_response.iter_content.return_value = [content]
        mock_response.raise_for_status = MagicMock()
        mocker.patch.object(
            self.api.session,
            "get",
            return_value=mock_response,
        )
        mocker.patch(
            "alephclient.exportdir.list_exports",
            return_value=[FAKE_EXPORT],
        )

    def test_download_to_file(self, mocker, tmp_path):
        dest = tmp_path / "output.zip"
        self._mock_download(mocker)
        result = download_export(self.api, "123", str(dest))
        assert result == dest
        assert dest.read_bytes() == b"file content"

    def test_download_to_directory(self, mocker, tmp_path):
        self._mock_download(mocker, content=b"data")
        result = download_export(self.api, "123", str(tmp_path))
        expected = tmp_path / "export.zip"
        assert result == expected
        assert expected.read_bytes() == b"data"
