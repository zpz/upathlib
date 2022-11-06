from pathlib import Path
from typing import Union

from ._upath import Upath
from ._local import LocalUpath
from .gcp import GcpBlobUpath


PathType = Union[str, Path, Upath]


def resolve_path(path: PathType,
                 *,
                 thread_pool_executors=None,
                 bucket_name=None,
                 project_id=None,
                 credentials=None,
                 ):
    if isinstance(path, str):
        if path.startswith('gs://'):
            return GcpBlobUpath(path,
                                thread_pool_executors=thread_pool_executors,
                                bucket_name=bucket_name,
                                project_id=project_id,
                                credentials=credentials,
            )
        path = Path(path)
    if isinstance(path, Path):
        return LocalUpath(str(path.absolute()), thread_pool_executors=thread_pool_executors)
    assert isinstance(path, Upath)
    return path
