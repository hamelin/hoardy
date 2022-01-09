from base64 import b32encode
from contextlib import contextmanager
import os
import sqlite3
import tempfile as tf
from typing import *  # noqa

import fsspec


_DB = sqlite3.Connection
PATH_DB = "db"


class Hoard:

    def __init__(self, protocol: str, **storage_options) -> None:
        self._prepend_root = lambda p: p
        root = storage_options.pop("root_local", None)
        if root is not None:
            self._prepend_root = lambda p: os.path.join(root, p)
        self._fs = fsspec.filesystem(protocol, **storage_options)

        while True:
            self._path_db_local = os.path.join(
                tf.gettempdir(),
                b32encode(os.urandom(10)).decode("utf-8")
            )
            if not os.path.isfile(self._path_db_local):
                break

        if self._fs.isfile(self._path_db):
            self._fs.get(self._path_db, self._path_db_local)
        if not os.path.isfile(self._path_db_local):
            with self._modifying_db() as db:
                _initialize_db(db)

    @classmethod
    def local(klass, root: os.PathLike) -> "Hoard":
        return klass(protocol="file", root_local=os.path.abspath(root))

    def __del__(self) -> None:
        if os.path.isfile(self._path_db_local):
            os.remove(self._path_db_local)

    @property
    def _path_db(self) -> os.PathLike:
        return self._prepend_root(PATH_DB)

    @contextmanager
    def _modifying_db(self) -> Iterator[_DB]:
        with self._fs.transaction:
            with sqlite3.connect(self._path_db_local) as db:
                yield db
            self._fs.put(self._path_db_local, self._path_db)


def _initialize_db(db: _DB) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS artifacts(
            uuid        TEXT    PRIMARY KEY NOT NULL,
            size        INTEGER,
            hash        TEXT,
            timestamp   TEXT,
            description TEXT,
            preview     TEXT
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS exclusions(
            feed TEXT PRIMARY KEY NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS log(
            timestamp TEXT    NOT NULL,
            action    INTEGER NOT NULL,
            uuid      TEXT             REFERENCES artifacts(uuid),
            feed      TEXT             REFERENCES exclusions(feed)
        );
        """
    )
