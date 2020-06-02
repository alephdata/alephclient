import logging
import threading
from queue import Queue
from pathlib import Path
from typing import Optional, Dict

from alephclient import settings
from alephclient.api import AlephAPI
from alephclient.errors import AlephException
from alephclient.util import backoff

log = logging.getLogger(__name__)


def _get_foreign_id(root_path: Path, path: Path) -> Optional[str]:
    if path == root_path:
        if path.is_dir():
            return None
        return path.name
    if root_path in path.parents:
        return str(path.relative_to(root_path))
    return None


def _upload_path(api: AlephAPI, path: Path, collection_id: str, parent_id: str,
                 foreign_id: str) -> str:
    metadata = {
        'foreign_id': foreign_id,
        'file_name': path.name,
    }
    log.info('Upload [%s->%s]: %s', collection_id, parent_id, foreign_id)
    if parent_id is not None:
        metadata['parent_id'] = parent_id
    result = api.ingest_upload(collection_id, path, metadata=metadata)
    if 'id' not in result:
        raise AlephException('Upload failed')
    return result['id']


def _crawl_path(q: Queue, api: AlephAPI, collection_id: str, parent_id: str,
                root_path: Path, path: Path):
    foreign_id = _get_foreign_id(root_path, path)
    # A foreign ID will be generated for all paths but the root directory of
    # an imported folder. For this, we'll just list the directory but not
    # create a document to reflect the root.
    if foreign_id is not None:
        parent_id = _upload_path(api, path, collection_id,
                                 parent_id, foreign_id)
    if path.is_dir():
        for child in path.iterdir():
            q.put((child, parent_id, 1))


def _upload(q: Queue, api: AlephAPI, collection_id: str, root_path: Path):
    while not q.empty():
        path, parent_id, try_number = q.get()
        try:
            _crawl_path(q, api, collection_id, parent_id, root_path, path)
        except AlephException as exc:
            if exc.transient and try_number < api.retries:
                backoff(exc, try_number)
                q.put((path, parent_id, try_number + 1))
            else:
                log.error(exc.message)
        except Exception:
            log.exception('Failed [%s]: %s', collection_id, path)
        q.task_done()


def crawl_dir(api: AlephAPI, path: str, foreign_id: str, config: Dict):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use.
    language: language hint for the documents
    """
    _path = Path(path).resolve()
    collection = api.load_collection_by_foreign_id(foreign_id, config)
    collection_id = collection.get('id')
    _queue: Queue = Queue()
    _queue.put((_path, None, 1))
    threads = []
    for i in range(settings.THREADS):
        args = (_queue, api, collection_id, _path)
        thread = threading.Thread(target=_upload, args=args)
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # block until all tasks are done
    _queue.join()
    for thread in threads:
        thread.join()
