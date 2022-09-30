"""
This module defines a base class for paths in a *cloud* storage, aka "blob store".
This is in contrast to a *local* disk storage, which is the subject of `_local.py`.
"""
import pathlib
from ._upath import Upath
from ._local import LocalUpath

from overrides import overrides, EnforceOverrides


def _resolve_local_path(p):
    if isinstance(p, str):
        p = pathlib.Path(p)
    if isinstance(p, pathlib.Path):
        p = LocalUpath(str(p.absolute()))
    else:
        assert isinstance(p, LocalUpath)
    return p


class BlobUpath(Upath, EnforceOverrides):
    @property
    def blob_name(self) -> str:
        return self._path.lstrip("/")

    def download_dir(self, target, *, overwrite=False, desc=None) -> int:
        target_ = _resolve_local_path(target)
        return self.export_dir(
            target_,
            overwrite=overwrite,
            desc=desc or f"Downloading from {self!r} into {target_!r}",
        )

    def download_file(self, target, *, overwrite=False) -> None:
        target_ = _resolve_local_path(target)
        return self.export_file(target_, overwrite=overwrite)

    @overrides
    def is_dir(self) -> bool:
        """In a typical blob store, there is no such concept as a
        "directory". Here we emulate the concept in a local file
        system. If there is a blob named like

            /ab/cd/ef/g.txt

        we say there exists directory "/ab/cd/ef".
        We should never have a trailing `/` in a blob's name, like

            /ab/cd/ef/

        (I don't know whether the blob stores allow
        such blob names.)

        Consequently, `is_dir` is equivalent
        to "having stuff in the dir". There is no such thing as
        an "empty directory" in blob stores.
        """
        try:
            next(self.iterdir())
            return True
        except StopIteration:
            return False

    @overrides
    def iterdir(self):
        # A naive, inefficient implementation.
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

    def upload_dir(self, source, *, overwrite=False, desc=None) -> int:
        s = _resolve_local_path(source)
        return self.import_dir(
            s, overwrite=overwrite, desc=desc or f"Uploading from {s!r} into {self!r}"
        )

    def upload_file(self, source, *, overwrite=False) -> None:
        s = _resolve_local_path(source)
        return self.import_file(s, overwrite=overwrite)
