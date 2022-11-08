__version__ = "0.6.8b2"

from pathlib import Path
from typing import Union

from ._upath import Upath, FileInfo, LockAcquireError, LockReleaseError
from ._local import LocalUpath
from ._blob import BlobUpath


PathType = Union[str, Path, Upath]


__all__ = [
    "Upath",
    "LocalUpath",
    "BlobUpath",
    "FileInfo",
    "LockAcquireError",
    "LockReleaseError",
    "PathType",
]
