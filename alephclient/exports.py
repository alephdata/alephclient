from pathlib import Path
from typing import List, Dict

from requests import RequestException
from requests.exceptions import HTTPError

from alephclient.api import AlephAPI, APIResultSet
from alephclient.errors import AlephException


def list_exports(api: AlephAPI) -> List[Dict]:
    """Fetch all exports from the API, handling pagination."""
    url = api._make_url("exports")
    return list(APIResultSet(api, url))


def format_exports_table(exports: List[Dict]) -> str:
    """Format a list of exports as a plain-text table."""
    if not exports:
        return "No exports found."

    headers = ["ID", "Label", "Status", "Created At", "Content Hash"]
    keys = ["id", "label", "status", "created_at", "content_hash"]

    rows = []
    for export in exports:
        rows.append([str(export.get(k, "")) for k in keys])

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(val))

    def format_row(values):
        return "  ".join(v.ljust(col_widths[i]) for i, v in enumerate(values))

    lines = [format_row(headers), format_row(["-" * w for w in col_widths])]
    for row in rows:
        lines.append(format_row(row))
    return "\n".join(lines)


def _get_export(api: AlephAPI, export_id: str) -> Dict:
    """Fetch a single export by ID from the exports list."""
    for export in list_exports(api):
        if str(export.get("id")) == str(export_id):
            return export
    raise AlephException(f"Export {export_id} not found")


def download_export(api: AlephAPI, export_id: str, destination: str) -> Path:
    """Download an export archive to the given destination path."""
    export = _get_export(api, export_id)
    download_url = export.get("links", {}).get("download")
    if not download_url:
        raise AlephException(f"No download link for export {export_id}")

    file_name = export.get("file_name", export_id)
    dest = Path(destination)
    if dest.is_dir():
        dest = dest / file_name
    dest.parent.mkdir(parents=True, exist_ok=True)

    try:
        response = api.session.get(download_url, stream=True)
        response.raise_for_status()
    except (RequestException, HTTPError) as exc:
        raise AlephException(exc) from exc

    with open(dest, "wb") as fh:
        for chunk in response.iter_content(chunk_size=512 * 1024):
            if chunk:
                fh.write(chunk)

    return dest
