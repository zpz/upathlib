# flake8: noqa

__version__ = "0.6.8b5"

from pathlib import Path
from typing import Union

from ._upath import Upath, FileInfo, LockAcquireError, LockReleaseError
from ._local import LocalUpath
from ._blob import BlobUpath

try:
    from .gcp import GcpBlobUpath
except ImportError:
    pass
try:
    from .azure import AzureBlobUpath
except ImportError:
    pass


PathType = Union[str, Path, Upath]


def resolve_path(path: PathType):
    if isinstance(path, str):
        if path.startswith("gs://"):
            from upathlib.gcp import GcpBlobUpath

            return GcpBlobUpath(path)
        path = Path(path)
    if isinstance(path, Path):
        return LocalUpath(str(path.absolute()))
    assert isinstance(path, Upath)
    return path
