import sqlite3
import time
from typing import *  # noqa

from hoardy import Hoard
from . import localdir, path_db


def test_hoard_create():
    with localdir() as dir:
        assert not path_db(dir).exists()
        Hoard.local(dir)
        print(path_db(dir))
        assert path_db(dir).is_file()
        db = sqlite3.connect(path_db(dir))
        try:
            cursor = db.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            assert {name for name, in cursor} == {"log", "artifacts", "exclusions"}
        finally:
            db.close()
