import os
import os.path
from pathlib import PurePosixPath
from typing import List, Union, Tuple, Iterator, TypeVar


T = TypeVar('T')


class Upath:
    '''
    Unlike `pathlib.Path`, which has the concept of
    "current working directory" implicitly determined by the
    execution environment, `Upath` does not have an implicit "cwd".
    Rather, it is explicitly specified by the argument `home`.
    '''

    def __init__(self, home: str, *parts: Union[str, os.PathLike]):
        self._home = os.path.normpath(home or '/')
        if parts:
            path_s = os.path.normpath(os.path.join(*parts))
            assert not path_s.startswith('.')
            if not path_s.startswith('/'):
                path_s = '/' + path_s
        else:
            path_s = '/'

        self._path = PurePosixPath(path_s)
        # The path is always "absolute" starting with '/'.

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._home}, {str(self.path).lstrip('/')})"

    def __str__(self) -> str:
        return str(self.fullpath)

    def _compare_(self, op, other):
        if not (other.__class__ is self.__class__):
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
        try:
            return self._hash
        except AttributeError:
            self._hash = hash(self.__str__())
            return self._hash

    def __truediv__(self: T, key: str) -> T:
        return self.joinpath(key)

    def __call__(self: T, *parts: str) -> T:
        return self.joinpath(*parts)

    def cd(self: T, relpath: str) -> T:
        '''Change home path; return self.'''
        assert str(self.path) == '/'
        self._home = os.path.normpath(os.path.join(self._home, relpath))
        return self

    def clear(self):
        assert not self.is_file()
        self.rm_rf()

    def exists(self) -> bool:
        raise NotImplementedError

    @property
    def fullpath(self) -> PurePosixPath:
        return PurePosixPath(os.path.normpath(os.path.join(
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
    def path(self) -> PurePosixPath:
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
            if not (other.__class__ is self.__class__):
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

    def with_name(self: T, name: str) -> T:
        return self.__class__(self._home, self.path.with_name(name))

    def with_stem(self: T, stem: str) -> T:
        # Available in Python 3.9+.
        return self.__class__(self._home, self.path.with_stem(stem))

    def with_suffix(self: T, suffix: str) -> T:
        return self.__class__(self._home, self.path.with_suffix(suffix))

    def write_bytes(self, data: bytes, parents: bool = False) -> int:
        '''Write bytes to the file, overwriting existing content if any.'''
        raise NotImplementedError

    def write_text(self, data: str, encoding=None, errors=None, parents: bool = False) -> int:
        '''Write text to the file, overwriting existing content if any.'''
        raise NotImplementedError


class Dropbox:
    def __init__(self, remote: Upath, local: Upath = None):
        pass

    def download(self, remote_file: str, local_file: str, overwrite: bool = False) -> None:
        raise NotImplementedError

    def upload(self, local_file: str, remote_file: str, overwrite: bool = False) -> None:
        raise NotImplementedError

    def download_dir(self, remote_dir: str, local_dir: str, overwrite: bool = False, verbose: bool = True) -> None:
        raise NotImplementedError

    def upload_dir(self, local_dir: str, remote_dir: str, overwrite: bool = False, verbose: bool = True) -> None:
        raise NotImplementedError
