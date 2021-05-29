from __future__ import annotations
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import asyncio
import logging
import os
import os.path
import pathlib
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from io import UnsupportedOperation
from typing import List, Union, Tuple, Iterator, TypeVar, AsyncIterator


logger = logging.getLogger(__name__)
T = TypeVar('T', bound='Upath')


class Upath:  # pylint: disable=too-many-public-methods
    '''
    Unlike `pathlib.Path`, which has the concept of
    "current working directory" implicitly determined by the
    execution environment, `Upath` does not have an implicit "cwd".
    Rather, it is explicitly specified by the argument `home`.

    A `Upath` consists of two parts: `home`, and `path` in `home`.
    The `path` treats `home` as the "root".

    There's only one way to change `Upath.home`, and that is via `Upath.cd`.
    This can both expand (entering a subdirectory) and contract
    (backing up to parent) the home path.

    There are multiple ways to change `Upath.path`. They fall under
    two categories: change directory, or change name only.

    `Upath.joinpath` will change directory. The `/` operator is
    equivalent. This can not go beyond the "root". For example,
    `Upath('my/home', 'ab/cd/ef').joinpath('..', '..', '..', '..)`
    will be `Upath('my/home', '/')`, not `Upath('my', '/')`.

    There are several methods to change the name alone, including
    `Upath.with_name`, `Upath.with_stem`, `Upath.with_suffix`.
    '''

    _executor: ThreadPoolExecutor = None

    def __init__(self, home: str, *parts: Union[str, os.PathLike]):
        assert home
        assert not home.startswith('.')
        home = os.path.normpath(home)
        self._home = '/' + home.lstrip('/')

        if parts:
            path_s = os.path.normpath(
                os.path.join(*parts))  # pylint: disable=no-value-for-parameter
            assert not path_s.startswith('.')
            path_s = '/' + path_s.lstrip('/')
        else:
            path_s = '/'
        self._path = path_s
        # The path is always "absolute" starting with '/'.

    def __copy__(self: T) -> T:
        return self.__class__(self._home, self._path)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self._home}', '{str(self.path).lstrip('/')}')"

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

    async def _a_do(self, func, *args, **kwargs):
        func = partial(func, *args, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, func)

    async def a_clear(self):
        return await self._a_do(self.clear)

    async def a_download(self, *args, **kwargs):
        return await self._a_do(self.download, *args, **kwargs)

    async def a_download_dir(self, *args, **kwargs):
        return await self._a_do(self.download_dir, *args, **kwargs)

    async def a_exists(self):
        return await self._a_do(self.exists)

    async def a_glob(self, pattern):
        raise NotImplementedError

    async def a_is_dir(self):
        return await self._a_do(self.is_dir)

    async def a_is_file(self):
        return await self._a_do(self.is_file)

    def a_iterdir(self):
        return self.a_glob('*')

    async def a_ls(self, *args, **kwargs):
        return await self._a_do(self.ls, *args, **kwargs)

    async def a_mkdir(self, *args, **kwargs):
        return await self._a_do(self.mkdir, *args, **kwargs)

    async def a_open(self, *args, **kwargs):
        return await self._a_do(self.open, *args, **kwargs)

    async def a_read_bytes(self):
        return await self._a_do(self.read_bytes)

    async def a_read_text(self, *args, **kwargs):
        return await self._a_do(self.read_text, *args, **kwargs)

    async def a_rename(self, *args, **kwargs):
        return await self._a_do(self.rename, *args, **kwargs)

    async def a_rglob(self, pattern):
        raise NotImplementedError

    async def a_rm(self, missing_ok=False):
        return await self._a_do(self.rm, missing_ok=missing_ok)

    async def a_rmdir(self):
        return await self._a_do(self.rmdir)

    async def a_rm_rf(self):
        return await self._a_do(self.rm_rf)

    async def a_stat(self):
        return await self._a_do(self.stat)

    async def a_upload(self, *args, **kwargs):
        return await self._a_do(self.upload, *args, **kwargs)

    async def a_upload_dir(self, *args, **kwargs):
        return await self._a_do(self.upload_dir, *args, **kwargs)

    async def a_write_bytes(self, *args, **kwargs):
        return await self._a_do(self.write_bytes, *args, **kwargs)

    async def a_write_text(self, *args, **kwargs):
        return await self._a_do(self.write_text, *args, **kwargs)

    def cd(self: T, relpath: str) -> T:
        '''Change home path; return self.'''
        if self._path != '/':
            raise UnsupportedOperation('`cd` can not be used on non-home path')
        self._home = os.path.normpath(os.path.join(self._home, relpath))
        return self

    def clear(self):
        '''Clears all content of the directory, but keep the directory.'''
        for p in self.iterdir():
            p.rm_rf()

    def download(self,
                 target: Union[str, pathlib.Path, Upath],
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
            if isinstance(target, pathlib.Path):
                target.unlink()
            else:
                target.rm()
            target.write_bytes(self.read_bytes())
            return 1
        if target.is_dir():
            raise FileExistsError(f"directory '{target}'")
        assert not target.exists()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.read_bytes())
        return 1

    def download_dir(self,
                     target: Union[str, pathlib.Path, Upath],
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
        '''Return the "full path" starting with `self.home`.'''
        return pathlib.PurePosixPath(
            os.path.normpath(
                os.path.join(
                    self._home,
                    self._path.lstrip('/'),
                )
            )
        )

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

    def ls(self: T, recursive: bool = False) -> List[T]:
        if not self.exists():
            return []
        if self.is_file():
            return [self]
        if recursive:
            return sorted(self.rglob('*'))
        return sorted(self.iterdir())

    def match(self, path_pattern: str) -> bool:
        return self.path.match(path_pattern)

    def mkdir(self: T, parents: bool = False, exist_ok: bool = False) -> T:
        '''Mutate self, and return self to facilitate chaining.'''
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
        return self.path.parts

    @property
    def path(self) -> pathlib.PurePosixPath:
        '''Return the path treating `self.home` as the root.'''
        return pathlib.PurePosixPath(self._path)

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

    def rename(self: T, target: Union[str, T], overwrite: bool = False) -> T:
        '''Mutate and return self.'''
        raise NotImplementedError

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

    def upload(self,
               source: Union[str, pathlib.Path, Upath],
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
                   source: Union[str, pathlib.Path, Upath],
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
    def __init__(self, *args):
        assert os.name == 'posix'
        if not args:
            super().__init__(str(pathlib.Path.home()))
        else:
            super().__init__(*args)

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

    @ property
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
