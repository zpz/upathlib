"""
This module defines a base class for paths in a *cloud* storage, aka "blob store".
This is in contrast to a *local* disk storage, which is the subject of `_local.py`.
"""
from __future__ import annotations
import pathlib
from collections.abc import Iterator

from ._upath import Upath, T
from ._local import LocalUpath, LocalPathType

from overrides import overrides, EnforceOverrides


def _resolve_local_path(p: LocalPathType):
    if isinstance(p, str):
        p = pathlib.Path(p)
    if isinstance(p, pathlib.Path):
        p = LocalUpath(str(p.resolve().absolute()))
    else:
        assert isinstance(p, LocalUpath)
    return p


class BlobUpath(Upath, EnforceOverrides):
    @property
    def blob_name(self) -> str:
        return self._path.lstrip("/")

    @overrides
    def is_dir(self) -> bool:
        """In a typical blob store, there is no such concept as a
        "directory". Here we emulate the concept in a local file
        system. If there is a blob named like

        ::

            /ab/cd/ef/g.txt

        we say there exists directory "/ab/cd/ef".
        We should never have a trailing `/` in a blob's name, like

        ::

            /ab/cd/ef/

        (I don't know whether the blob stores allow
        such blob names.)

        Consequently, ``is_dir`` is equivalent
        to "having stuff in the dir". There is no such thing as
        an "empty directory" in blob stores.
        """
        try:
            next(self.iterdir())
            return True
        except StopIteration:
            return False

    @overrides
    def iterdir(self: T) -> Iterator[T]:
        """
        Yield immediate children under the current dir.

        This is a naive, inefficient implementation.
        Expected to be refined by subclasses.
        """
        p0 = self._path  # this could be '/'.
        if not p0.endswith("/"):
            p0 += "/"
        np0 = len(p0)
        subdirs = set()
        for p in self.riterdir():
            tail = p._path[np0:]
            if tail.startswith("/"):
                raise Exception(f"malformed blob name '{p._path}'")
            if "/" in tail:
                tail = tail[: tail.find("/")]
            if tail not in subdirs:
                yield self / tail
                subdirs.add(tail)

    def download_dir(self, target: LocalPathType, **kwargs) -> int:
        """
        A specialization of :meth:`~upathlib.Upath.export_dir` where the target location
        is on the local disk.
        """
        target_ = _resolve_local_path(target)
        return self.export_dir(target_, **kwargs)

    def download_file(self, target: LocalPathType, *, overwrite=False) -> None:
        """
        A specialization of :meth:`~upathlib.Upath.export_file` where the target location
        is on the local disk.
        """
        target_ = _resolve_local_path(target)
        return self.export_file(target_, overwrite=overwrite)

    def upload_dir(self, source: LocalPathType, **kwargs) -> int:
        """
        A specialization of :meth:`~upathlib.Upath.import_dir` where the source location
        is on the local disk.
        """
        s = _resolve_local_path(source)
        return self.import_dir(s, **kwargs)

    def upload_file(self, source: LocalPathType, *, overwrite=False) -> None:
        """
        A specialization of :meth:`~upathlib.Upath.import_file` where the source location
        is on the local disk.
        """
        s = _resolve_local_path(source)
        return self.import_file(s, overwrite=overwrite)
