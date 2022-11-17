# flake8: noqa

__version__ = "0.6.8"

from pathlib import Path
from typing import Union

from ._upath import Upath, FileInfo, LockAcquireError, LockReleaseError
from ._local import LocalUpath
from ._blob import BlobUpath

try:
    from .gcs import GcsBlobUpath
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
            # If you encounter a "gs://..." path but
            # you haven't installed GCS dependencies,
            # you'll get an exception!
            return GcsBlobUpath(path)
        if path.startswith("s3://"):
            raise NotImplementedError("AWS S3 storage is not implemented")
        if path.startswith("https://"):
            if "blob.core.windows.net" in path:
                return AzureBlobUpath(path)
            raise ValueError(path)
        path = Path(path)
    if isinstance(path, Path):
        return LocalUpath(str(path.resolve().absolute()))
    assert isinstance(path, Upath)
    return path
