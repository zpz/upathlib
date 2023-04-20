"""
The package *upathlib*
defines a unified API for cloud blob store (aka "object store") as well as local file systems.

Attention is focused on identifying the *most essential* functionalities
while working with a blob store for data processing.
Functionalities in a traditional local file system that are secondary in these tasks---such
as symbolic links, fine-grained permissions, and various access modes---are ignored.

End user should look to the class :class:`~upathlib.Upath` for documentation of the API.
Local file system is implemented by :class:`LocalUpath`, which subclasses Upath.
Client for Google Cloud Storage (i.e. blob store on GCP) is implemented by another Upath subclass,
namely :class:`~upathlib.GcsBlobUpath`.

One use case is the package `biglist <https://biglist.readthedocs.io/en/latest/>`__,
where the class `Biglist <https://biglist.readthedocs.io/en/latest/#biglist.Biglist>`__ takes a Upath object to indicate its location of storage.
It does not care whether the storage is local or in a cloud blob store---it
simply uses the common API to operate the storage.
"""

__version__ = "0.7.8b3"

from pathlib import Path
from typing import Union

from ._blob import BlobUpath
from ._local import LocalPathType, LocalUpath
from ._upath import FileInfo, LockAcquireError, LockReleaseError, Upath

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
    if not isinstance(path, Upath):
        raise TypeError(f"`{type(path)}` is provided while `{PathType}` is expected")
    return path
