import contextlib
import datetime
import logging
import os
import os.path
import pathlib

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

    def file_info(self):
        try:
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
        except FileNotFoundError:
            return

    def isdir(self):
        return self.localpath.is_dir()

    def isfile(self):
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

    def rename(self, target, *, overwrite=False):
        target = self / target
        if target == self:
            return self

        if not self.exists():
            raise FileNotFoundError(self)

        if target.isfile():
            if not overwrite:
                raise FileExistsError(target)
        elif target.isdir():
            if list(target.iterdir()):  # dir not empty
                raise FileExistsError(target)
            target.rmdir()
        else:
            assert not target.exists()
        self.localpath.rename(target.localpath)
        return target

    def riterdir(self):
        for p in self.iterdir():
            if p.isfile():
                yield p
            elif p.isdir():
                yield from p.riterdir()

    def rmdir(self, *, missing_ok=False, concurrency=None):
        def _rmdir(path):
            n = 0
            for p in path.iterdir():
                if p.isfile():
                    logger.info('deleting %s', p.localpath)
                    p.localpath.unlink()
                    n += 1
                else:
                    n += _rmdir(p)
            path.localpath.rmdir()  # this is a `pathlib.Path` call
            return n

        if self.isdir():
            n = _rmdir(self)

            p = self.parent
            while not list(p.iterdir()):  # empty dir
                p.localpath.rmdir()
                p = p.parent

            return n

        if missing_ok:
            return 0

        raise FileNotFoundError(self.localpath)

    def rmfile(self, *, missing_ok=False):
        if self.isfile():
            logger.info('deleting %s', self.localpath)
            self.localpath.unlink()

            p = self.parent
            while not list(p.iterdir()):  # empty dir
                p.localpath.rmdir()
                p = p.parent

            return 1

        if missing_ok:
            return 0

        raise FileNotFoundError(self)

    def write_bytes(self, data: bytes, *, overwrite=False):
        if self.localpath.is_file():
            if not overwrite:
                raise FileExistsError(self)
        elif self.localpath.is_dir():
            raise IsADirectoryError(self)
        else:
            self.localpath.parent.mkdir(exist_ok=True, parents=True)
        return self.localpath.write_bytes(data)
