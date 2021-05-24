from __future__ import annotations
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import logging
import os
import os.path
import pathlib
from typing import List, Union, Tuple, Iterator, TypeVar


logger = logging.getLogger(__name__)
T = TypeVar('T', bound='Upath')


class Upath:  # pylint: disable=too-many-public-methods
    '''
    Unlike `pathlib.Path`, which has the concept of
    "current working directory" implicitly determined by the
    execution environment, `Upath` does not have an implicit "cwd".
    Rather, it is explicitly specified by the argument `home`.
    '''

    def __init__(self, home: str, *parts: Union[str, os.PathLike]):
        self._home = os.path.normpath(home or '/')
        if parts:
            path_s = os.path.normpath(os.path.join(  # pylint: disable=E1120
                *parts))  # pylint: disable=no-value-for-parameter
            assert not path_s.startswith('.')
            if not path_s.startswith('/'):
                path_s = '/' + path_s
        else:
            path_s = '/'

        self._path = pathlib.PurePosixPath(path_s)
        # The path is always "absolute" starting with '/'.

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._home}, {str(self.path).lstrip('/')})"

    def __str__(self) -> str:
        return str(self.fullpath)

    def _compare_(self, op, other):
        if other.__class__ is not self.__class__:
            return NotImplemented
        return op(str(other))

    def __eq__(self, other) -> bool:
        return self._compare_(self.__str__().__eq__, other)

    def __lt__(self, other) -> bool:
        return self._compare_(self.__str__().__lt__, other)

    def __le__(self, other) -> bool:
        return self._compare_(self.__str__().__le__, other)

    def __gt__(self, other) -> bool:
        return self._compare_(self.__str__().__gt__, other)

    def __ge__(self, other) -> bool:
        return self._compare_(self.__str__().__ge__, other)

    def __hash__(self) -> int:
        return hash(self.__str__())

    def __truediv__(self: T, key: str) -> T:
        return self.joinpath(key)

    def cd(self: T, relpath: str) -> T:
        '''Change home path; return self.'''
        assert str(self.path) == '/'
        self._home = os.path.normpath(os.path.join(self._home, relpath))
        return self

    def clear(self):
        assert not self.is_file()
        self.rm_rf()

    def download(self,
                 target: Union[str, pathlib.Path, 'Upath'],
                 overwrite: bool = False) -> int:
        '''This provides a fallback implementation.

        Subclasses should provide more efficient implementations
        if possible.
        '''
        assert self.is_file()
        if isinstance(target, str):
            target = pathlib.Path(target)

        # `target` is either a dir, in which case
        # file will be copied into it, or a file (existent or not),
        # in which case file will be copied to it.
        if target.is_dir():
            target = target / self.name
        if target.is_file():
            if not overwrite:
                raise FileExistsError(str(target))
            target.unlink()
            target.write_bytes(self.read_bytes())
            return 1
        if target.is_dir():
            raise FileExistsError(f"directory '{target}'")
        assert not target.exists()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.read_bytes())
        return 1

    def download_dir(self,
                     target: Union[str, pathlib.Path, 'Upath'],
                     overwrite: bool = False):
        '''This provides a fallback implementation.

        Subclasses should provide more efficient implementations
        if possible.
        '''
        assert self.is_dir()
        if isinstance(target, str):
            target = pathlib.Path(target)
        if isinstance(target, pathlib.Path):
            target = LocalUpath('/', target.absolute())
        else:
            assert isinstance(target, Upath)
        if target.is_file():
            raise FileExistsError(f"file '{target}'")
        target.mkdir(parents=True, exist_ok=True)

        n = 0
        for s in self.iterdir():
            ss = s.relative_to(self)
            if s.is_file():
                n += s.download(target / ss, overwrite=overwrite)
            else:
                n += s.download_dir(target / ss, overwrite=overwrite)
        return n

    def exists(self) -> bool:
        raise NotImplementedError

    @property
    def fullpath(self) -> pathlib.PurePosixPath:
        return pathlib.PurePosixPath(os.path.normpath(os.path.join(
            self._home, str(self.path).lstrip('/'))))

    def glob(self: T, pattern: str) -> Iterator[T]:
        # Implemented when `is_dir()` returns `True.
        raise NotImplementedError

    def home(self: T) -> T:
        return self.__class__(self._home)

    def is_dir(self) -> bool:
        raise NotImplementedError

    def is_file(self) -> bool:
        raise NotImplementedError

    def is_relative_to(self: T, other: Union[str, T] = None) -> bool:
        try:
            self.relative_to(other)
            return True
        except ValueError:
            return False

    def iterdir(self: T) -> Iterator[T]:
        return self.glob('*')

    def joinpath(self: T, *parts: str) -> T:
        return self.__class__(self._home, self.path.joinpath(*parts))

    def ls(self: T) -> List[T]:
        if not self.exists():
            return []
        if self.is_file():
            return [self]
        return sorted(self.iterdir())

    def ls_r(self: T) -> List[T]:
        '''Same as `ls`, but recursively.'''
        if not self.exists():
            return []
        if self.is_file():
            return [self]
        return sorted(self.rglob('*'))

    def match(self, path_pattern: str) -> bool:
        return self.path.match(path_pattern)

    def mkdir(self: T, parents: bool = False, exist_ok: bool = False) -> T:
        '''Mutate self, and return self to facilitate chaining.'''
        raise NotImplementedError

    def mv(self: T, target: Union[str, T], overwrite: bool = False) -> T:
        '''Mutate and return self.'''
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self.path.name

    def open(self, mode: str = 'r'):
        raise NotImplementedError

    @property
    def parent(self: T) -> T:
        return self.__class__(self._home, self.path.parent)

    @property
    def parts(self) -> Tuple[str, ...]:
        return self._path.parts

    @property
    def path(self) -> pathlib.PurePosixPath:
        return self._path

    def read_bytes(self) -> bytes:
        raise NotImplementedError

    def read_text(self, encoding: str = None, errors: str = None):
        # Refer to https://docs.python.org/3/library/functions.html#open
        raise NotImplementedError

    def relative_to(self: T, other: Union[str, T] = None) -> str:
        if not other or other == '/':
            return str(self.path).lstrip('/')

        if isinstance(other, str):
            other = self / other
        else:
            if other.__class__ is not self.__class__:
                raise ValueError('`other` must be either a string or an object of class {}'.format(
                    self.__class__.__name__
                ))
        return str(self.fullpath.relative_to(other.fullpath))

    def rglob(self: T, pattern: str) -> Iterator[T]:
        raise NotImplementedError

    def rm(self, missing_ok: bool = False) -> int:
        '''Removes this file. Return number of files removed.

        If the path points to a directory, use `rmdir` instead.
        '''
        raise NotImplementedError

    def rmdir(self) -> None:
        '''The directory must be empty.'''
        raise NotImplementedError

    def rm_rf(self) -> int:
        '''Analogous to `rm -rf`.

        Return number of files removed.
        '''
        if not self.exists():
            return 0
        if self.is_file():
            self.rm()
            return 1
        k = 0
        for v in self.iterdir():
            n = v.rm_rf()
            k += n
        self.rmdir()
        return k

    @property
    def root(self) -> str:
        return self._home

    def stat(self):
        raise NotImplementedError

    @property
    def stem(self) -> str:
        return self.path.stem

    @property
    def suffix(self) -> str:
        return self.path.suffix

    @property
    def suffixes(self) -> List[str]:
        return self.path.suffixes

    def unlink(self, missing_ok: bool = False) -> int:
        '''Provided as a synonym to `rm`.

        Subclass should implement `rm`.
        '''
        return self.rm(missing_ok=missing_ok)

    def upload(self,
               source: Union[str, pathlib.Path, 'Upath'],
               overwrite: bool = False) -> int:
        '''This provides a fallback implementation.

        Subclasses should provide more efficient implementations
        if possible.
        '''
        if isinstance(source, str):
            source = pathlib.Path(source)
        if isinstance(source, pathlib.Path):
            source = LocalUpath('/', str(source.absolute()))
        else:
            assert isinstance(source, Upath)
        return source.download(self, overwrite=overwrite)

    def upload_dir(self,
                   source: Union[str, pathlib.Path, 'Upath'],
                   overwrite: bool = False):
        '''This provides a fallback implementation.

        Subclasses should provide more efficient implementations
        if possible.
        '''
        if isinstance(source, str):
            source = pathlib.Path(source)
        if isinstance(source, pathlib.Path):
            source = LocalUpath('/', str(source.absolute()))
        else:
            assert isinstance(source, Upath)
        return source.download_dir(self, overwrite=overwrite)

    def with_name(self: T, name: str) -> T:
        return self.__class__(self._home, self.path.with_name(name))

    # def with_stem(self: T, stem: str) -> T:
    #     # Available in Python 3.9+.
    #     return self.__class__(self._home, self.path.with_stem(stem))

    def with_suffix(self: T, suffix: str) -> T:
        return self.__class__(self._home, self.path.with_suffix(suffix))

    def write_bytes(self, data: bytes, parents: bool = False) -> int:
        '''Write bytes to the file, overwriting existing content if any.'''
        raise NotImplementedError

    def write_text(self, data: str, encoding=None, errors=None, parents: bool = False) -> int:
        '''Write text to the file, overwriting existing content if any.'''
        raise NotImplementedError


class LocalUpath(Upath):  # pylint: disable=abstract-method
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
            assert target.root == self.root
        target = target.fullpath
        if target.exists() and not overwrite:
            raise FileExistsError
        self.localpath.rename(target)
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
