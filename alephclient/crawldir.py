import logging
import threading
import re
import os
from os import PathLike
from queue import Queue
from pathlib import Path
from typing import cast, Optional, Dict

from alephclient.api import AlephAPI
from alephclient.errors import AlephException
from alephclient.util import backoff

log = logging.getLogger(__name__)


class CrawlDirectory(object):
    def __init__(
        self,
        api: AlephAPI,
        collection: Dict,
        path: Path,
        index: bool = True,
        nojunk: bool = False,
    ):
        self.api = api
        self.index = index
        self.exclude = (
            {
                "f": re.compile(r"\..*|thumbs\.db|desktop\.ini", re.I),
                "d": re.compile(r"\..*|\$recycle\.bin|system volume information", re.I),
            }
            if nojunk
            else None
        )
        self.collection = collection
        self.collection_id = cast(str, collection.get("id"))
        self.root = path
        self.queue: Queue = Queue()
        self.scan_queue: Queue = Queue()
        if path.is_dir():
            if not self.is_excluded(path):
                self.scan_queue.put((path, None))
        elif not self.is_excluded(path):
            self.queue.put((path, None))

    def crawl(self):
        while not self.scan_queue.empty():
            path, parent_id = self.scan_queue.get()
            id = None
            foreign_id = self.get_foreign_id(Path(path))
            if foreign_id is not None:
                id = self.backoff_ingest_upload(path, parent_id, foreign_id)
            self.scandir(path, id, parent_id)
            self.scan_queue.task_done()

    def consume(self):
        while True:
            path, parent_id = self.queue.get()
            # None value for path is used as poison, to signal end.
            if path is None:
                self.queue.task_done()
                break
            self.backoff_ingest_upload(path, parent_id, self.get_foreign_id(Path(path)))
            self.queue.task_done()

    def is_excluded(self, path: PathLike) -> bool:
        # The exclude pattern is constructed bearing in mind that will
        # be called using fullmatch.
        if self.exclude is None:
            return False

        path = Path(path)

        if path.is_dir():
            return self.exclude["d"].fullmatch(path.name) is not None
        return self.exclude["f"].fullmatch(path.name) is not None

    def scandir(self, path: Path, id: str, parent_id: str):
        with os.scandir(path) as iterator:
            while True:
                child = next(iterator, None)
                if child is None:
                    break
                if self.is_excluded(child):
                    continue
                if child.is_dir():
                    # Use a separate scan queue to avoid calling scandir recursively.
                    self.scan_queue.put((child, id))
                else:
                    self.queue.put((child, id))

    def get_foreign_id(self, path: Path) -> Optional[str]:
        if path == self.root:
            if path.is_dir():
                return None
            return path.name

        # path.is_relative_to is still a bit new, so opting for something... older
        try:
            return str(path.relative_to(self.root))
        except ValueError:
            return None

    def backoff_ingest_upload(
        self, path: Path, parent_id: str, foreign_id: str
    ) -> Optional[str]:
        try_number = 1
        while True:
            try:
                return self.ingest_upload(Path(path), parent_id, foreign_id)
            except AlephException as err:
                if err.transient and try_number < self.api.retries:
                    try_number += 1
                    backoff(err, try_number)
                else:
                    log.error(err.message)
                    return None
            except Exception:
                log.exception("Failed [%s]: %s", self.collection_id, path)
                return None

    def ingest_upload(self, path: Path, parent_id: str, foreign_id: str) -> str:
        metadata = {
            "foreign_id": foreign_id,
            "file_name": path.name,
        }
        log.info("Upload [%s->%s]: %s", self.collection_id, parent_id, foreign_id)
        if parent_id is not None:
            metadata["parent_id"] = parent_id
        result = self.api.ingest_upload(
            self.collection_id,
            path,
            metadata=metadata,
            index=self.index,
        )
        if "id" not in result and not hasattr(result, "id"):
            raise AlephException("Upload failed")
        return result["id"]


def crawl_dir(
    api: AlephAPI,
    path: str,
    foreign_id: str,
    config: Dict,
    index: bool = True,
    nojunk: bool = False,
    parallel: int = 1,
):
    """Crawl a directory and upload its content to a collection

    params
    ------
    path: path of the directory
    foreign_id: foreign_id of the collection to use.
    language: language hint for the documents
    """
    root = Path(path).resolve()
    collection = api.load_collection_by_foreign_id(foreign_id, config)
    crawler = CrawlDirectory(api, collection, root, index=index, nojunk=nojunk)
    consumers = []

    # Use one thread to produce using scandir and at least one to consume
    # files for upload.
    producer = threading.Thread(target=crawler.crawl, daemon=True)
    producer.start()
    for i in range(max(1, parallel)):
        consumer = threading.Thread(target=crawler.consume, daemon=True)
        consumer.start()
        consumers.append(consumer)

    # Block until the producer is done with queueing the tree.
    producer.join()

    # Block until the file upload queue is drained.
    crawler.queue.join()

    # Poison the queue to signal end to each consumer.
    for consumer in consumers:
        crawler.queue.put((None, None))

    # Block until all file upload queue consumers are done.
    for consumer in consumers:
        consumer.join()
