import logging
import threading
from six.moves.queue import Queue

from alephclient import settings
from alephclient.errors import AlephException
from alephclient.util import Path, backoff

log = logging.getLogger(__name__)


def _get_foreign_id(root_path, path):
    if path == root_path:
        if path.is_dir():
            return
        return path.name
    if root_path in path.parents:
        return str(path.relative_to(root_path))


def _upload_path(api, path, collection_id, parent_id, foreign_id):
    metadata = {
        'foreign_id': foreign_id,
        'file_name': path.name,
    }
    log.info('Upload [%s->%s]: %s', collection_id, parent_id, foreign_id)
    if parent_id is not None:
        metadata['parent_id'] = parent_id
    result = api.ingest_upload(collection_id, path, metadata=metadata)
    return result.get('id')


def _crawl_path(q, api, collection_id, parent_id, root_path, path):
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


def _upload(q, api, collection_id, root_path):
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


def crawl_dir(api, path, foreign_id, config):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use.
    language: language hint for the documents
    """
    path = Path(path).resolve()
    collection = api.load_collection_by_foreign_id(foreign_id, config)
    collection_id = collection.get('id')
    q = Queue()
    q.put((path, None, 1))
    threads = []
    for i in range(settings.THREADS):
        args = (q, api, collection_id, path)
        t = threading.Thread(target=_upload, args=args)
        t.daemon = True
        t.start()
        threads.append(t)

    # block until all tasks are done
    q.join()
    for t in threads:
        t.join()
