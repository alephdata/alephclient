from alephclient.api import AlephAPI
import os

from alephclient.crawldir import CrawlDirectory
from pathlib import Path


class TestCrawlDirectory:
    base_path = os.path.abspath("alephclient/tests/testdata")

    def test_get_foreign_id_with_dir(self):
        path = Path(os.path.join(self.base_path, "jan/week1"))

        crawldir = CrawlDirectory(AlephAPI, {}, path)
        foreign_id = crawldir.get_foreign_id(path)
        assert foreign_id == None

    def test_get_foreign_id_with_file(self):
        path = Path(os.path.join(self.base_path, "feb/2.txt"))

        crawldir = CrawlDirectory(AlephAPI, {}, path)
        foreign_id = crawldir.get_foreign_id(path)
        assert foreign_id == "2.txt"

    def test_get_foreign_id_different_path(self):
        path = Path(os.path.join(self.base_path, "lib/test.txt"))

        crawldir = CrawlDirectory(AlephAPI, {}, path)
        crawldir.root = self.base_path

        foreign_id = crawldir.get_foreign_id(path)

        assert foreign_id == "lib/test.txt"
