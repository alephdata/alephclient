import os
import re

from alephclient.api import AlephAPI
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

    def test_is_excluded(self):
        path = Path(os.path.join(self.base_path, "jan/week1"))
        crawldir = CrawlDirectory(AlephAPI, {}, path)
        is_excluded = crawldir.is_excluded(path)
        assert is_excluded == False

    def test_is_excluded_no_exclude(self):
        path = Path(os.path.join(self.base_path, "jan/week1"))
        crawldir = CrawlDirectory(AlephAPI, {}, path)
        crawldir.exclude = None
        is_excluded = crawldir.is_excluded(path)
        assert is_excluded == False

    def test_is_excluded_exclude_dir(self):
        path = Path(os.path.join(self.base_path, "jan/week1"))
        crawldir = CrawlDirectory(AlephAPI, {}, path, nojunk=True)
        crawldir.exclude["d"] = re.compile(r"week1\/*", re.I)
        is_excluded = crawldir.is_excluded(path)
        assert is_excluded == True
