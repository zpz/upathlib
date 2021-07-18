import contextlib
import datetime
import logging
import os
import os.path
import pathlib
import shutil

import filelock
# `filelock` is also called `py-filelock`.
# Tried `fasteners` also. In one use case,
# `filelock` worked whereas `fasteners.InterprocessLock` failed.
#
# Other options to lock into include
# `oslo.concurrency`, `pylocker`, `portalocker`.

from ._upath import Upath, LockAcquisitionTimeoutError, FileInfo


logging.getLogger('filelock').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class LocalUpath(Upath):  # pylint: disable=abstract-method
    def __init__(self, *pathsegments: str):
        assert os.name == 'posix'
        if pathsegments:
            parts = [str(pathlib.Path(*pathsegments).absolute())]
        else:
            parts = [str(pathlib.Path.cwd().absolute())]
        super().__init__(*parts)

    def copy_file(self, target, *, overwrite=False):
        if not self.is_file():
            raise FileNotFoundError(self)
        target = self.parent / target
        if self == target:
            return self
        if target.is_file():
            if not overwrite:
                raise FileExistsError(target)
            target.remove_file()
        elif target.is_dir():
            raise FileExistsError(target)
        else:
            assert not target.exists()
        os.makedirs(target.localpath.parent, exist_ok=True)
        shutil.copy(self.localpath, target.localpath)
        return target

    def _export_file(self, target: Upath, *, overwrite=False):
        if isinstance(target, LocalUpath):
            self.copy_file(str(target), overwrite=overwrite)
            return
        # Call the other side in case it implements an efficient
        # file upload.
        target._import_file(self, overwrite=overwrite)

    def file_info(self):
        if not self.is_file():
            return
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

    def _import_file(self, source: Upath, *, overwrite=False):
        if isinstance(source, LocalUpath):
            source.copy_file(str(self), overwrite=overwrite)
            return
        # Call the other side in case it implements an efficient
        # file download.
        source._export_file(self, overwrite=overwrite)

    def is_dir(self):
        return self.localpath.is_dir()

    def is_file(self):
        return self.localpath.is_file()

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
    def lock(self, *, wait=60):
        lock = filelock.FileLock(str(self.localpath))
        try:
            lock.acquire(timeout=wait)
            yield
        except filelock.Timeout as e:
            raise LockAcquisitionTimeoutError(str(self)) from e
        finally:
            lock.release()

    def read_bytes(self):
        try:
            return self.localpath.read_bytes()
        except (IsADirectoryError, FileNotFoundError) as e:
            raise FileNotFoundError(self) from e

    def remove_dir(self, *, missing_ok=False, concurrency=None):
        n = super().remove_dir(missing_ok=True, concurrency=concurrency)

        def _remove_dir(path):
            for p in path.iterdir():
                assert p.is_dir()
                _remove_dir(p)
            path.rmdir()

        if self.is_dir():
            _remove_dir(self.localpath)
        elif not missing_ok:
            raise FileNotFoundError(self)

        return n

    def remove_file(self, *, missing_ok=False):
        if self.is_file():
            logger.info('deleting %s', self.localpath)
            self.localpath.unlink()
            return 1

        if missing_ok:
            return 0

        raise FileNotFoundError(self)

    def rename_dir(self, target, *, overwrite=False):
        if not self.is_dir():
            raise FileNotFoundError(self)
        target = self.parent / target

        if target.is_file():
            if not overwrite:
                raise FileExistsError(target)
        elif target.is_dir():
            if list(target.iterdir()):  # dir not empty
                raise FileExistsError(target)
            target.remove_dir()
        else:
            assert not target.exists()
        self.localpath.rename(target.localpath)
        return target

    def rename_file(self, target, *, overwrite=False):
        if not self.is_file():
            raise FileNotFoundError(self)
        target = self.parent / target
        if target == self:
            return self

        if target.is_file():
            if not overwrite:
                raise FileExistsError(target)
        elif target.is_dir():
            if list(target.iterdir()):  # dir not empty
                raise FileExistsError(target)
            target.remove_dir()
        else:
            assert not target.exists()
        os.makedirs(target.localpath.parent, exist_ok=True)
        self.localpath.rename(target.localpath)
        return target

    def riterdir(self):
        for p in self.iterdir():
            if p.is_file():
                yield p
            elif p.is_dir():
                yield from p.riterdir()

    def write_bytes(self, data: bytes, *, overwrite=False):
        if self.localpath.is_file():
            if not overwrite:
                raise FileExistsError(self)
        elif self.localpath.is_dir():
            raise IsADirectoryError(self)
        else:
            self.localpath.parent.mkdir(exist_ok=True, parents=True)
        return self.localpath.write_bytes(data)

    # async def a_exists(self):
    #     return self.exists()

    # async def a_file_info(self):
    #     return self.file_info()

    # async def a_is_dir(self):
    #     return self.is_dir()

    # async def a_is_file(self):
    #     return self.is_file()

    # async def a_remove_file(self, *, missing_ok=False):
    #     return self.remove_file(missing_ok=missing_ok)

    # async def a_rename_dir(self, target, *, overwrite=False):
    #     return self.rename_dir(target, overwrite=overwrite)

    # async def a_rename_file(self, target, *, overwrite=False):
    #     return self.rename_file(target, overwrite=overwrite)
