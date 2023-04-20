from __future__ import annotations

import pathlib
import sys
from collections.abc import Iterator

from typing_extensions import Self

from ._local import LocalPathType, LocalUpath
from ._upath import Upath


def _resolve_local_path(p: LocalPathType):
    if isinstance(p, str):
        p = pathlib.Path(p)
    if isinstance(p, pathlib.Path):
        p = LocalUpath(str(p.resolve().absolute()))
    else:
        assert isinstance(p, LocalUpath)
    return p


class BlobUpath(Upath):
    """
    BlobUpath is a base class for paths in a *cloud* storage, aka "blob store".
    This is in contrast to a *local* disk storage, which is implemnted by :class:`LocalUpath`.
    """

    @property
    def blob_name(self) -> str:
        """
        Return the "name" of the blob. This is the "path" without a leading ``'/'``.
        In cloud blob stores, this is exactly the name of the blob. The name often
        contains ``'/'``, which has no special role in the name per se but is *interpreted*
        by users to be a directory separator.
        """
        return self._path.lstrip("/")

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

    def iterdir(self) -> Iterator[Self]:
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

    def download_dir(
        self,
        target: LocalPathType,
        *,
        overwrite: bool = False,
        quiet: bool = False,
        **kwargs,
    ) -> int:
        """
        A specialization of :meth:`~upathlib.Upath.copy_dir` where the target location
        is on the local disk.
        """
        target = _resolve_local_path(target)

        if not quiet:
            print(f"Downloading from {self!r} into {target!r}", file=sys.stderr)
        return self._copy_dir(
            self,
            target,
            "download_file",
            overwrite=overwrite,
            quiet=quiet,
            **kwargs,
        )

    def download_file(self, target: LocalPathType, *, overwrite=False) -> None:
        """
        A specialization of :meth:`~upathlib.Upath.copy_file` where the target location
        is on the local disk.

        Subclass is expected to override with a more efficient implementation.
        """
        self.copy_file(target, overwrite=overwrite)

    def upload_dir(
        self,
        source: LocalPathType,
        *,
        overwrite: bool = False,
        quiet: bool = False,
        **kwargs,
    ) -> int:
        """
        A specialization of :meth:`~upathlib.Upath.copy_dir` for :class:`~upathlib.LocalUpath`
        where the target location is in a cloud blob store.
        """
        source = _resolve_local_path(source)
        if not quiet:
            print(f"Importing from {source!r} into {self!r}", file=sys.stderr)
        return self._copy_dir(
            source,
            self,
            "upload_file",
            overwrite=overwrite,
            quiet=quiet,
            reversed=True,
            **kwargs,
        )

    def upload_file(self, source: LocalPathType, *, overwrite=False) -> None:
        """
        A specialization of :meth:`~upathlib.Upath.copy_file` for :class:`~upathlib.LocalUpath`
        where the target location is in a cloud blob store.

        Subclass is expected to override with a more efficient implementation.
        """
        source.copy_file(self, overwrite=overwrite)
