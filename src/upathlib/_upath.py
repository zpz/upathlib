from __future__ import annotations
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import abc
import asyncio
import contextlib
import json
import logging
import os
import os.path
import pathlib
import pickle
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from io import UnsupportedOperation
from typing import List, Union, Iterator, TypeVar

import filelock
# `filelock` is also called `py-filelock`.
# Tried `fasteners` also. In one use case,
# `filelock` worked whereas `fasteners.InterprocessLock` failed.
#
# Other options to lock into include
# `oslo.concurrency`, `pylocker`, `portalocker`.

logging.getLogger('filelock').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
T = TypeVar('T', bound='Upath')


class Upath(abc.ABC):  # pylint: disable=too-many-public-methods
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

    Primary methods/attributes for getting the components of the object
    include `home`, `path`, `fullpath`, `root`.
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
        return f"{self.__class__.__name__}('{self._home}', '{self._path.lstrip('/')}')"

    def __str__(self) -> str:
        return str(self.fullpath)

    def __eq__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        return self._home == other._home and self._path == other._path

    def __lt__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if self._home < other._home:
            return True
        if self._home > other._home:
            return False
        return self._path < other._path

    def __le__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if self._home < other._home:
            return True
        if self._home > other._home:
            return False
        return self._path <= other._path

    def __gt__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if self._home > other._home:
            return True
        if self._home < other._home:
            return False
        return self._path > other._path

    def __ge__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if self._home > other._home:
            return True
        if self._home < other._home:
            return False
        return self._path >= other._path

    def __hash__(self) -> int:
        return hash((self._home, self._path))

    def __truediv__(self: T, key: str) -> T:
        return self.joinpath(key)

    async def _a_do(self, func, *args, **kwargs):
        func = partial(func, *args, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, func)

    async def a_clear(self, *args, **kwargs):
        return await self._a_do(self.clear, *args, **kwargs)

    async def a_cp(self, *args, **kwargs):
        return await self._a_do(self.cp, *args, **kwargs)

    async def a_cp_from(self, *args, **kwargs):
        return await self._a_do(self.cp_from, *args, **kwargs)

    async def a_exists(self):
        return await self._a_do(self.exists)

    async def a_is_dir(self):
        return await self._a_do(self.is_dir)

    async def a_is_file(self):
        return await self._a_do(self.is_file)

    async def a_iterdir(self, *, missing_ok=False):
        # This is a suboptimal reference implementation.
        for p in self.iterdir(missing_ok=missing_ok):
            yield p

    async def a_mkdir(self, *args, **kwargs):
        return await self._a_do(self.mkdir, *args, **kwargs)

    async def a_mv(self, *args, **kwargs):
        return await self._a_do(self.mv, *args, **kwargs)

    async def a_read_bytes(self):
        return await self._a_do(self.read_bytes)

    async def a_read_json(self, **kwargs):
        return await self._a_do(self.read_json, **kwargs)

    async def a_read_pickle(self):
        return await self._a_do(self.read_pickle)

    async def a_read_text(self, **kwargs):
        return await self._a_do(self.read_text, **kwargs)

    async def a_rm(self, *args, **kwargs):
        return await self._a_do(self.rm, *args, **kwargs)

    async def a_rmdir(self, *args, **kwargs):
        return await self._a_do(self.rmdir, *args, **kwargs)

    async def a_rmrf(self):
        return await self._a_do(self.rmrf)

    async def a_stat(self):
        return await self._a_do(self.stat)

    async def a_write_bytes(self, data, **kwargs):
        return await self._a_do(self.write_bytes, data, **kwargs)

    async def a_write_json(self, data, **kwargs):
        return await self._a_do(self.write_json, data, **kwargs)

    async def a_write_pickle(self, data, **kwargs):
        return await self._a_do(self.write_pickle, data, **kwargs)

    async def a_write_text(self, data, **kwargs):
        return await self._a_do(self.write_text, data, **kwargs)

    def cd(self: T, relpath: str) -> T:
        '''Change home path; return self.'''
        if self._path != '/':
            raise UnsupportedOperation('`cd` can not be used on non-home path')
        self._home = os.path.normpath(os.path.join(self._home, relpath))
        return self

    def clear(self, *, missing_ok: bool = False):
        '''Clear all contents of the directory, but keep the directory.

        If the path does not exist, and `missing_ok` is False,
        raise FileNotFoundError.

        If the path is a file, raise NotADirectoryError.
        '''
        for p in self.iterdir(missing_ok=missing_ok):
            p.rmrf()

    def cp(self,
           target: Union[str, pathlib.Path, Upath],
           *,
           overwrite: bool = False) -> int:
        '''Copy the file or directory as or into the specified `target`.
        Return number of files copied.

        This is like "export" or "download".

        This provides a fallback implementation.
        Subclasses should provide more efficient implementations
        if possible, while maintainer the behavior defined in
        this implementation.
        '''
        if not self.exists():
            raise FileNotFoundError(str(self))

        if isinstance(target, str):
            target = pathlib.Path(target)
        if isinstance(target, pathlib.Path):
            target = LocalUpath('/', target.absolute())
        else:
            assert isinstance(target, Upath)

        if target == self:
            return 0

        if target.is_dir():
            target = target / self.name
        if target.is_file():
            if not overwrite:
                # TODO:
                # In cloud blobstores, this may not be a problem.
                raise FileExistsError(str(target))
            target.rm()
        else:
            assert not target.exists()

        if self.is_file():
            target.write_bytes(self.read_bytes())
            return 1

        n = 0
        for s in self.iterdir():
            name = s.name
            if s.is_file():
                # If `overwrite` is True, existing target files
                # are overwritten. Otherwise, they are skipped.
                # If `overwrite` is False, existing target files
                # will cause exceptions.
                if not overwrite and (target / name).is_file():
                    k = 0
                else:
                    s.cp(target / name, overwrite=overwrite)
                    k = 1
            else:
                k = s.cp(target / name, overwrite=overwrite)
            n += k
        return n

    def cp_from(self,
                source: Union[str, pathlib.Path, Upath],
                *,
                overwrite: bool = False) -> int:
        '''Opposite of `cp`. This is like "import" or "upload".'''
        if isinstance(source, str):
            source = pathlib.Path(source)
        if isinstance(source, pathlib.Path):
            source = LocalUpath('/', str(source.absolute()))
        else:
            assert isinstance(source, Upath)
        return source.cp(self, overwrite=overwrite)

    @abc.abstractmethod
    def exists(self) -> bool:
        '''In a blobstore with blobs

            /a/b/cd
            /a/b/cd/e.txt

        '/a/b/cd' exists, and is both a file and a dir;
        '/a/b/cd/e.txt' exists, and is a file;
        '/a/b' exists, and is a dir;
        '/a/b/c' does not exist.
        '''
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

    def home(self: T) -> T:
        return self.__class__(self._home)

    @abc.abstractmethod
    def is_dir(self) -> bool:
        '''Return `True` if the path is an existing directory,
        `False` if an existing non-directory.

        Return `None` if the path does not exist.'''
        raise NotImplementedError

    @abc.abstractmethod
    def is_file(self) -> bool:
        '''Return `True` if the path is an existing file,
        `False` if an existing non-file.

        Return `None` if the path does not exist.'''
        raise NotImplementedError

    @abc.abstractmethod
    def iterdir(self: T, *, missing_ok: bool = False) -> Iterator[T]:
        '''When the path points to a directory, yield path objects of the
        directory contents. Only one level down; not recursively.

        If the path does not exist, and `missing_ok` is False,
        raise FileNotFoundError. If `missing_ok` is True, return an empty
        iterator.

        If the path is a file, raise NotADirectoryError.'''
        raise NotImplementedError

    # def lsdir(self: T, *, missing_ok: bool = False) -> List[T]:
    #     return list(self.iterdir(missing_ok=missing_ok))

    def joinpath(self: T, *other: str) -> T:
        '''Join this path with more segments, return the new path object.'''
        return self.__class__(self._home, self.path.joinpath(*other))

    @contextlib.contextmanager
    @abc.abstractmethod
    def lock(self: T, *, wait: float = 60) -> T:
        '''Lock the file pointed to, in order to have exclusive access.
        Return self.

        File locking is a tricky matter. The semantics of this method
        will likely see some iterations.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(self: T, *, parents: bool = False, exist_ok: bool = False) -> T:
        '''Create a new directory at this given path. Return `self`.

        If the directory already exists, and `exist_ok` is False,
        raise FileExistsError.

        If the path exists but is a file, raise FileExistsError.

        If `parents` is False, a missing parent raises FileNotFoundError.'''
        raise NotImplementedError

    def mv(self: T, target: Union[str, T], *, overwrite: bool = False) -> T:
        '''Rename this file or directory to the given `target`, and
        return a new Upath instance pointing to `target`.

        If `target` exists and is a directory, then current path is
        moved into it.

        If `target` exists and is a file, then

            If `self` is a file, then `target`
            is overwritten if `overwrite` is True, otherwise
            FileExistsError is raised.

            If `self` is a directory, then FileExistsError
            is raised.

        If `target` does not exist, then it will be the name
        of the new path.'''
        raise NotImplementedError
        # TODO: an inefficient fallback implementation
        # could be provided.

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def parent(self: T) -> T:
        return self.__class__(self._home, self.path.parent)

    @property
    def path(self) -> pathlib.PurePosixPath:
        '''Return the path under `self.home`, treating
        the latter as the root.

        Methods of the returned object could be useful,
        such as `parts`, `match`.
        '''
        return pathlib.PurePosixPath(self._path)

    @abc.abstractmethod
    def read_bytes(self) -> bytes:
        '''Return the binary contents of the pointed-to file.

        If `self` is a directory, raise IsADirectoryError.

        If `self` does not exist, raise FileNotFoundError.
        '''
        raise NotImplementedError

    def read_json(self, **kwargs):
        return json.loads(self.read_text(**kwargs))

    def read_pickle(self):
        return pickle.loads(self.read_bytes())

    def read_text(self, *, encoding: str = 'utf-8', errors: str = 'strict'):
        # Refer to https://docs.python.org/3/library/functions.html#open
        return self.read_bytes().decode(encoding=encoding, errors=errors)

    @abc.abstractmethod
    def rm(self, *, missing_ok: bool = False) -> int:
        '''Removes the file pointed to by `self`. Return number of files removed.

        If the file does not exist, and `missing_ok` is False,
        raise FileNotFoundError.

        If `self` is a directory, raise IsADirectoryError.
        In this case, use `rmdir` instead.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def rmdir(self, *, missing_ok: bool = False) -> None:
        '''Remove the empty directory pointed to by `self`.

        If the directory is not empty, raise OSError.

        If the directory does not exist and `missing_ok` is False,
        raise FileNotFoundError.

        If the object is not a directory, raise NotADirectoryError.'''
        raise NotImplementedError

    def rmrf(self) -> int:
        '''Analogous to `rm -rf`. Return number of files removed.

        The object pointed to by `self` may be either a file
        or a directory. If file, remove it. If directory,
        remove its contents recursively, and the directory itself.

        Compare `rmrf` with `clear`, which removes content in the directory
        but keeps the directory itself. Also, `clear` does not work
        on a file.
        '''
        if not self.exists():
            return 0
        if self.is_file():
            self.rm()
            return 1
        k = 0
        for v in self.iterdir():
            n = v.rmrf()
            k += n
        self.rmdir()
        return k

    @property
    def root(self) -> str:
        return self._home

    @abc.abstractmethod
    def stat(self) -> os.stat_result:
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

    # def with_stem(self: T, stem: str) -> T:
    #     # Available in Python 3.9+.
    #     return self.__class__(self._home, self.path.with_stem(stem))

    def with_suffix(self: T, suffix: str) -> T:
        return self.__class__(self._home, self.path.with_suffix(suffix))

    @abc.abstractmethod
    def write_bytes(self,
                    data: bytes,
                    *,
                    overwrite: bool = False) -> int:
        '''
        Write bytes to file. Parent directories are created as needed.

        `overwrite`: overwrite existing file?
            If False, and file exists, raises FileExistsError.

        If the object pointed to is a directory, then it may or
        may not be a problem depending on the file system.
        For example, with a cloud blobstore, this may not be a problem.
        With a local file system, this will raise IsADirectoryError.
        Note, `overwrite` has no effect on existing directory.

        Return number of bytes written.
        '''
        raise NotImplementedError

    def write_json(self, data: str, overwrite=False, **kwargs) -> int:
        return self.write_text(json.dumps(data),
                               overwrite=overwrite,
                               **kwargs)

    def write_pickle(self, data, *, overwrite=False) -> int:
        return self.write_bytes(
            pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL),
            overwrite=overwrite,
        )

    def write_text(self,
                   data: str,
                   *,
                   overwrite: bool = False,
                   encoding='utf-8',
                   errors='strict',
                   ) -> int:
        '''
        Return number of characters written.
        '''
        n = len(data)
        z = data.encode(encoding=encoding, errors=errors)
        self.write_bytes(z, overwrite=overwrite)
        return n


class LocalUpath(Upath):  # pylint: disable=abstract-method
    def __init__(self, *args):
        assert os.name == 'posix'
        if not args:
            super().__init__(str(pathlib.Path.cwd().absolute()))
        else:
            super().__init__(*args)

    def exists(self):
        return self.localpath.exists()

    def is_dir(self):
        if not self.exists():
            return None
        return self.localpath.is_dir()

    def is_file(self):
        if not self.exists():
            return None
        return self.localpath.is_file()

    def iterdir(self, *, missing_ok=False):
        if not self.exists() and missing_ok:
            return
        else:
            for p in self.localpath.iterdir():
                yield self / p.name

    @ property
    def localpath(self) -> pathlib.Path:
        return pathlib.Path(str(self.fullpath))

    @contextlib.contextmanager
    def lock(self, *, wait=60):
        lock = filelock.FileLock(str(self) + '.__lock__')
        try:
            lock.acquire(timeout=wait)
            yield self
        finally:
            lock.release()

    def mkdir(self, *, parents=False, exist_ok=False):
        self.localpath.mkdir(parents=parents, exist_ok=exist_ok)
        return self

    def mv(self, target, *, overwrite=False):
        if isinstance(target, str):
            target = self / target
        else:
            assert target.__class__ is self.__class__
            assert target.root == self.root
        if target == self:
            return self
        if target.exists() and not overwrite:
            raise FileExistsError(str(target))
        self.localpath.rename(target.fullpath)
        return target

    def read_bytes(self):
        return self.localpath.read_bytes()

    def rm(self, *, missing_ok=False) -> int:
        if not self.exists():
            if missing_ok:
                return 0
            raise FileNotFoundError(str(self.fullpath))
        logger.debug('deleting %s', self.localpath)
        self.localpath.unlink()
        return 1

    def rmdir(self, *, missing_ok=False):
        logger.debug('deleting %s/', self.localpath)
        if not self.exists():
            if missing_ok:
                return
            raise FileNotFoundError(str(self.fullpath))
        self.localpath.rmdir()

    def stat(self):
        return self.localpath.stat()

    def write_bytes(self, data: bytes, *, overwrite=False):
        if self.is_file():
            if not overwrite:
                raise FileExistsError(str(self))
        else:
            self.parent.mkdir(parents=True, exist_ok=True)
        return self.localpath.write_bytes(data)
