from contextlib import contextmanager
import gc
from pathlib import Path
import shutil
import tempfile as tf
from typing import *  # noqa

from hoardy import PATH_DB


@contextmanager
def localdir() -> Iterator[Path]:
    dir = tf.mkdtemp()
    try:
        yield Path(dir)
    finally:
        shutil.rmtree(dir)


def path_db(dir: Path) -> Path:
    return dir / PATH_DB
