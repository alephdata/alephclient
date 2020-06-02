import logging
import threading
from queue import Queue
from pathlib import Path
from typing import cast, Optional, Dict

from alephclient import settings
from alephclient.api import AlephAPI
from alephclient.errors import AlephException
from alephclient.util import backoff

log = logging.getLogger(__name__)


class CrawlDirectory(object):

    def __init__(self, api: AlephAPI, collection: Dict, path: Path,
                 index: bool = True):
        self.api = api
        self.index = index
        self.collection = collection
        self.collection_id = cast(str, collection.get('id'))
        self.root = path
        self.queue: Queue = Queue()
        self.queue.put((path, None, 1))

    def execute(self):
        while not self.queue.empty():
            path, parent_id, try_number = self.queue.get()
            try:
                self.crawl_path(parent_id, path)
            except AlephException as exc:
                if exc.transient and try_number < self.api.retries:
                    backoff(exc, try_number)
                    self.queue.put((path, parent_id, try_number + 1))
                else:
                    log.error(exc.message)
            except Exception:
                log.exception('Failed [%s]: %s', self.collection_id, path)
            finally:
                self.queue.task_done()

    def get_foreign_id(self, path: Path) -> Optional[str]:
        if path == self.root:
            if path.is_dir():
                return None
            return path.name
        if self.root in path.parents:
            return str(path.relative_to(self.root))
        return None

    def upload_path(self, path: Path, parent_id: str, foreign_id: str) -> str:
        metadata = {
            'foreign_id': foreign_id,
            'file_name': path.name,
        }
        log.info('Upload [%s->%s]: %s', self.collection_id,
                 parent_id, foreign_id)
        if parent_id is not None:
            metadata['parent_id'] = parent_id
        result = self.api.ingest_upload(self.collection_id, path,
                                        metadata=metadata,
                                        index=self.index)
        if 'id' not in result:
            raise AlephException('Upload failed')
        return result['id']

    def crawl_path(self, parent_id: str, path: Path):
        foreign_id = self.get_foreign_id(path)
        # A foreign ID will be generated for all paths but the root directory
        # of an imported folder. For this, we'll just list the directory but
        # not create a document to reflect the root.
        if foreign_id is not None:
            parent_id = self.upload_path(path, parent_id, foreign_id)
        if path.is_dir():
            for child in path.iterdir():
                self.queue.put((child, parent_id, 1))


def crawl_dir(api: AlephAPI, path: str, foreign_id: str,
              config: Dict, index: bool = True):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use.
    language: language hint for the documents
    """
    root = Path(path).resolve()
    collection = api.load_collection_by_foreign_id(foreign_id, config)
    crawler = CrawlDirectory(api, collection, root, index=index)
    threads = []
    for i in range(settings.THREADS):
        thread = threading.Thread(target=crawler.execute)
        thread.daemon = True
        thread.start()
        threads.append(thread)

    # block until all tasks are done
    crawler.queue.join()
    for thread in threads:
        thread.join()
