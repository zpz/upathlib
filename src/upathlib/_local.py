import contextlib
import datetime
import os
import os.path
import pathlib
import shutil

import filelock

# `filelock` is also called `py-filelock`.
# Tried `fasteners` also. In one use case,
# `filelock` worked whereas `fasteners.InterprocessLock` failed.
#
# Other options to look into include
# `oslo.concurrency`, `pylocker`, `portalocker`.
from overrides import overrides

from ._upath import Upath, LockAcquireError, FileInfo


# End user may want to do this:
# logging.getLogger("filelock").setLevel(logging.WARNING)


class LocalUpath(Upath):
    def __init__(self, *pathsegments: str, **kwargs):
        assert os.name == "posix"
        if pathsegments:
            parts = [str(pathlib.Path(*pathsegments).absolute())]
        else:
            parts = [str(pathlib.Path.cwd().absolute())]

        super().__init__(*parts, **kwargs)

    @overrides
    def _copy_file(self, target, *, overwrite=False):
        if not overwrite and target.is_file():
            raise FileExistsError(target)
        os.makedirs(target.localpath.parent, exist_ok=True)
        shutil.copyfile(self.localpath, target.localpath)
        # If target already exists, it will be overwritten.

    @overrides
    def export_dir(self, target: Upath, **kwargs) -> int:
        if isinstance(target, LocalUpath):
            return super().export_dir(target, **kwargs)
        # `target` is a cloud store; it might have implemented
        # efficient 'download' functionality.
        return target.import_dir(self, **kwargs)

    @overrides
    def export_file(self, target: Upath, *, overwrite=False):
        if isinstance(target, LocalUpath):
            return self._copy_file(target, overwrite=overwrite)
        # `target` is a cloud store; it might have implemented
        # efficient 'upload' functionality.
        target.import_file(self, overwrite=overwrite)

    @overrides
    def file_info(self):
        if not self.is_file():
            return None
        st = self.localpath.stat()
        return FileInfo(
            ctime=st.st_ctime,
            mtime=st.st_mtime,
            time_created=datetime.datetime.fromtimestamp(st.st_ctime),
            time_modified=datetime.datetime.fromtimestamp(st.st_mtime),
            size=st.st_size,
            details=st,
        )
        # If an existing file is written to again using `write_...`,
        # then its `ctime` and `mtime` are both updated.
        # My experiments showed that `ctime` and `mtime` are equal.

    @overrides
    def import_dir(self, source: Upath, **kwargs) -> int:
        if isinstance(source, LocalUpath):
            return super().import_dir(source, **kwargs)
        return source.export_dir(self, **kwargs)

    @overrides
    def import_file(self, source: Upath, *, overwrite=False):
        if isinstance(source, LocalUpath):
            return source._copy_file(self, overwrite=overwrite)
        # Call the other side in case it implements an efficient
        # file download.
        source.export_file(self, overwrite=overwrite)

    @overrides
    def is_dir(self) -> bool:
        return self.localpath.is_dir()

    @overrides
    def is_file(self) -> bool:
        return self.localpath.is_file()

    @overrides
    def iterdir(self):
        try:
            for p in self.localpath.iterdir():
                yield self / p.name
        except (NotADirectoryError, FileNotFoundError):
            pass

    @property
    def localpath(self) -> pathlib.Path:
        return pathlib.Path(self._path)

    @contextlib.contextmanager
    @overrides
    def lock(self, *, timeout=None):
        os.makedirs(self.localpath.parent, exist_ok=True)
        lock = filelock.FileLock(str(self.localpath))
        try:
            lock.acquire(timeout=timeout)
            yield
        except filelock.Timeout as e:
            raise LockAcquireError(str(self)) from e
        finally:
            lock.release()

    @overrides
    def read_bytes(self) -> bytes:
        try:
            return self.localpath.read_bytes()
        except (IsADirectoryError, FileNotFoundError) as e:
            raise FileNotFoundError(self) from e

    @overrides
    def remove_dir(self, **kwargs) -> int:
        n = super().remove_dir(**kwargs)
        if self.localpath.is_dir():
            shutil.rmtree(self.localpath)
        return n

    @overrides
    def remove_file(self) -> None:
        self.localpath.unlink()

    @overrides
    def rename_dir(self, target, **kwargs):
        target_ = super().rename_dir(target, **kwargs)

        def _remove_empty_dir(path):
            k = 0
            for p in path.iterdir():
                if p.is_dir():
                    k += _remove_empty_dir(p)
                else:
                    k += 1
            if k == 0:
                path.rmdir()
            return k

        _remove_empty_dir(self.localpath)

        return target_

    @overrides
    def _rename_file(self, target: str, *, overwrite=False):
        target = self.parent / target
        if not overwrite and target.is_file():
            raise FileExistsError(target)
        os.makedirs(target.localpath.parent, exist_ok=True)
        self.localpath.rename(target.localpath)

    @overrides
    def riterdir(self):
        for p in self.iterdir():
            if p.is_file():
                yield p
            elif p.is_dir():
                yield from p.riterdir()

    @overrides
    def write_bytes(self, data: bytes, *, overwrite=False):
        if self.is_file():
            if not overwrite:
                raise FileExistsError(self)
        self.parent.localpath.mkdir(exist_ok=True, parents=True)
        self.localpath.write_bytes(data)
        # If `self` is an existing directory, will raise `IsADirectoryError`.
        # If `self` is an existing file, will overwrite.
