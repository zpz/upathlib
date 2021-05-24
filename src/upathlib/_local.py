import logging
import os
import pathlib
from ._upath import Upath

logger = logging.getLogger(__name__)


class LocalUPath(Upath):
    def __init__(self, *args, **kwargs):
        assert os.name == 'posix'
        super().__init__(*args, **kwargs)

    def _from_abs(self, abspath: pathlib.PosixPath):
        return self.__class__(
            self._home, str(abspath.absolute().relative_to(self._home)))

    def exists(self):
        return self.localpath.exists()

    def glob(self, pattern):
        for v in self.localpath.glob(pattern):
            yield self._from_abs(v)

    def is_dir(self):
        return self.localpath.is_dir()

    def is_file(self):
        return self.localpath.is_file()

    @property
    def localpath(self) -> pathlib.Path:
        return pathlib.Path(str(self.fullpath))

    def mkdir(self, parents=False, exist_ok=False):
        self.localpath.mkdir(parents=parents, exist_ok=exist_ok)
        return self

    def mv(self, target, overwrite=False):
        if isinstance(target):
            target = self / target
        else:
            assert target.__class__ is self.__class__
            assert target._home == self._home
        target = target.fullpath
        if target.exists() and not overwrite:
            raise FileExistsError
        self.fullpath().rename(target)
        return self

    def open(self, mode='r'):
        return self.localpath.open(mode=mode)

    def read_bytes(self):
        return self.localpath.read_bytes()

    def read_text(self, encoding=None, errors=None):
        return self.localpath.read_text(encoding=encoding, errors=errors)

    def rglob(self, pattern):
        for v in self.localpath.rglob(pattern):
            yield self._from_abs(v)

    def rm(self, missing_ok=False) -> int:
        if not self.exists():
            if missing_ok:
                return 0
            raise FileNotFoundError(str(self.fullpath))
        logger.debug('deleting %s', self.localpath)
        self.localpath.unlink()
        return 1

    def rmdir(self):
        logger.debug('deleting %s/', self.localpath)
        self.localpath.rmdir()

    def stat(self):
        return self.localpath.stat()

    def write_bytes(self, data: bytes, parents=False):
        if parents:
            self.parent.mkdir(parents=True, exist_ok=True)
        return self.localpath.write_bytes(data)

    def write_text(self, data: str, encoding=None, errors=None, parents=False):
        if parents:
            self.parent.mkdir(parents=True, exist_ok=True)
        return self.localpath.write_text(
            data, encoding=encoding, errors=errors)
