import contextlib
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

from ._upath import Upath, LockAcquisitionTimeoutError


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

    def exists(self):
        return self.localpath.exists()

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
        return self.localpath.read_bytes()

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
            if p.is_file():
                yield p
            elif p.is_dir():
                yield from p.riterdir()

    def rm(self, *, missing_ok=False):
        if self.is_file():
            logger.info('deleting %s', self.localpath)
            self.localpath.unlink()
            return 1

        if missing_ok:
            return 0

        raise FileNotFoundError(str(self.localpath))

    def rmdir(self, *, missing_ok=False):
        if self.is_dir():
            n = 0
            for p in self.iterdir():
                if p.is_file():
                    n += p.rm()
                else:
                    n += p.rmdir()
            self.localpath.rmdir()  # this is a `pathlib.Path` call
            return n

        if missing_ok:
            return 0

        raise NotADirectoryError(str(self.localpath))

    def stat(self):
        return self.localpath.stat()

    def write_bytes(self, data: bytes, *, overwrite=False):
        if self.is_file():
            if not overwrite:
                raise FileExistsError(self)
        else:
            self.parent.mkdir(exist_ok=True)
        return self.localpath.write_bytes(data)
