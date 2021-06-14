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
    _executor: ThreadPoolExecutor = None

    def __init__(self, *parts: Union[str, os.PathLike], **kwargs):
        self._path = os.path.normpath(os.path.join(
            '/', *parts))  # pylint: disable=no-value-for-parameter
        # The path is always "absolute" starting with '/'.
        # Unless it is `/`, it does not have a trailing `/`.
        self._kwargs = kwargs

    def __copy__(self: T) -> T:
        return self.__class__(self._path, **self._kwargs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self._path}')"

    def __str__(self) -> str:
        return self._path

    def __eq__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        return self._path == other._path

    def __lt__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        return self._path < other._path

    def __le__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        return self._path <= other._path

    def __gt__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        return self._path > other._path

    def __ge__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        return self._path >= other._path

    def __hash__(self) -> int:
        return hash(self._path)

    def __truediv__(self: T, key: str) -> T:
        return self.joinpath(key)

    def __rtruediv__(self: T, key: str) -> T:
        return self.joinpath(key)

    async def _a_do(self, func, *args, **kwargs):
        func = partial(func, *args, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, func)

    async def a_clear(self, *args, **kwargs):
        return await self._a_do(self.clear, *args, **kwargs)

    async def a_copy_in(self, *args, **kwargs):
        return await self._a_do(self.copy_in, *args, **kwargs)

    async def a_copy_out(self, *args, **kwargs):
        return await self._a_do(self.copy_out, *args, **kwargs)

    async def a_cp(self, *args, **kwargs):
        return await self._a_do(self.cp, *args, **kwargs)

    async def a_exists(self):
        return await self._a_do(self.exists)

    async def a_is_dir(self):
        return await self._a_do(self.is_dir)

    async def a_is_file(self):
        return await self._a_do(self.is_file)

    async def a_iterdir(self):
        # This is a suboptimal reference implementation.
        for p in self.iterdir():
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

    def clear(self):
        '''Clear all contents of the directory, but keep the directory.

        If the path is not a directory, raise NotADirectoryError.
        '''
        for p in self.iterdir():
            p.rmrf()

    def copy_in(self,
                source: Union[str, pathlib.Path, Upath],
                *,
                exist_action: str = None) -> int:
        '''This is like "import" or "upload".'''
        if isinstance(source, str):
            source = pathlib.Path(source)
        if isinstance(source, pathlib.Path):
            source = LocalUpath('/', str(source.absolute()))
        else:
            assert isinstance(source, Upath)
        return source.copy_out(self, exist_action=exist_action)

    def copy_out(self,
                 target: Union[str, pathlib.Path, Upath],
                 *,
                 exist_action: str = None) -> int:
        '''Copy the file or directory as or into the specified `target`.
        Return number of files copied.

        `exist_action`: mainly about file rather than directory.

        This provides a fallback implementation.
        Subclasses should provide more efficient implementations
        if possible, while maintains the behavior defined in
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

        if exist_action is None:
            exist_action = 'raise'
        else:
            assert exist_action in ('raise', 'skip', 'overwrite')

        if target.is_dir() or str(target.path) == '/':
            target = target / self.name

        if self.is_file():
            if target.is_file():
                if exist_action == 'raise':
                    raise FileExistsError(str(target))
                if exist_action == 'skip':
                    return 0
                target.write_bytes(self.read_bytes(), overwrite=True)
                return 1
            if target.is_dir():
                # Do not delete.
                raise FileExistsError(str(target))
            target.write_bytes(self.read_bytes(), overwrite=True)
            return 1

        if not self.is_dir():
            return 0

        if target.is_file():
            if exist_action == 'overwrite':
                target.rm()
            else:
                # Either 'raise' or 'skip'.
                raise FileExistsError(str(target))

        n = 0
        for s in self.iterdir():
            k = s.copy_out(target / s.name, exist_action=exist_action)
            n += k

        return n

    def cp(self: T, target: str, overwrite: bool = False) -> T:
        raise NotImplementedError

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

        If the directory does not exist, the behavior is different
        between cloud blob stores and local file systems.'''
        raise NotImplementedError

    def joinpath(self: T, *other: str) -> T:
        '''Join this path with more segments, return the new path object.'''
        return self.__class__(self.path.joinpath(*other), **self._kwargs)

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
    def mkdir(self, *, parents: bool = False, exist_ok: bool = False) -> None:
        '''Create a new directory at this given path.

        If the directory already exists, and `exist_ok` is False,
        raise FileExistsError.

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
        # This reference implementation uses copy/delete
        # to achieve the effect of renaming.
        # Concrete subclasses may have an efficient way
        # to conduct renaming.

        if isinstance(target, str):
            target = self / target
        else:
            assert target.__class__ is self.__class__
            # Subclass may assert other things before calling
            # this `super()` implementation.
        if target == self:
            return self

        if self.is_file():
            if target.is_file():
                if overwrite:
                    raise FileExistsError(str(target))
                target.write_bytes(self.read_bytes())
            elif target.is_dir():
                target = target / self.name
                if target.is_file():
                    if overwrite:
                        raise FileExistsError(str(target))
                target.write_bytes(self.read_bytes())
            else:
                target.write_bytes(self.read_bytes())
            self.rm()
            return target

        if self.is_dir():
            if target.is_dir():
                target = target / self.name
            for p in self.iterdir():
                p.mv(target / p.name)
            self.rmdir()
            return target

        raise FileNotFoundError(str(self))

    @property
    def name(self) -> str:
        # If `self.path` is '/', then `self.path.name` is ''.
        return self.path.name

    @property
    def parent(self: T) -> T:
        return self.__class__(str(self.path.parent), **self._kwargs)

    @property
    def path(self) -> pathlib.PurePosixPath:
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
    def rmdir(self) -> None:
        '''Remove the empty directory pointed to by `self`.

        If the directory is not empty, raise OSError.

        In other cases, the behavior may differ between
        cloud blob stores and local file systems.'''
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
        # Subclass may enforce conditions e.g. this can not be
        # conducted at the very root, for safty reasons.
        k = 0
        if self.is_file():
            self.rm()
            k += 1

        if self.is_dir():
            for v in self.iterdir():
                n = v.rmrf()
                k += n
            self.rmdir()
        return k

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
        return self.__class__(str(self.path.with_name(name)), **self._kwargs)

    # def with_stem(self: T, stem: str) -> T:
    #     # Available in Python 3.9+.
    #     return self.__class__(str(self.path.with_stem(stem)), **self._kwargs)

    def with_suffix(self: T, suffix: str) -> T:
        return self.__class__(str(self.path.with_suffix(suffix)), **self._kwargs)

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
    def __init__(self, *parts: str):
        assert os.name == 'posix'
        if parts:
            parts = [str(pathlib.Path(*parts).absolute())]
        else:
            parts = [str(pathlib.Path.cwd().absolute())]
        super().__init__(*parts)

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

    def iterdir(self):
        for p in self.localpath.iterdir():
            yield self / p.name

    @ property
    def localpath(self) -> pathlib.Path:
        return pathlib.Path(self._path)

    @contextlib.contextmanager
    def lock(self, *, wait=60):
        lock = filelock.FileLock(str(self.localpath) + '.__lock__')
        try:
            lock.acquire(timeout=wait)
            yield self
        finally:
            lock.release()

    def mkdir(self, *, parents=False, exist_ok=False):
        self.localpath.mkdir(parents=parents, exist_ok=exist_ok)

    def mv(self, target, *, overwrite=False):
        if isinstance(target, str):
            target = self / target
        else:
            assert target.__class__ is self.__class__
        if target == self:
            return self
        if target.exists() and not overwrite:
            raise FileExistsError(str(target))
        self.localpath.rename(target.localpath)
        return target

    def read_bytes(self):
        return self.localpath.read_bytes()

    def rm(self, *, missing_ok=False) -> int:
        if not self.exists():
            if missing_ok:
                return 0
            raise FileNotFoundError(str(self.localpath))
        logger.info('deleting %s', self.localpath)
        self.localpath.unlink()
        return 1

    def rmdir(self):
        if not self.exists():
            raise FileNotFoundError(str(self.localpath))
        self.localpath.rmdir()

    def rmrf(self):
        if self._path == '/':
            raise UnsupportedOperation("`rmrf` not allowed on root directory")
        return super().rmrf()

    def stat(self):
        return self.localpath.stat()

    def write_bytes(self, data: bytes, *, overwrite=False):
        if self.is_file():
            if not overwrite:
                raise FileExistsError(str(self))
        else:
            self.parent.mkdir(parents=True, exist_ok=True)
        return self.localpath.write_bytes(data)


class BlobUpath(Upath):  # pylint: disable=abstract-method
    @abc.abstractmethod
    def _blob_exists(self) -> bool:
        # Unless `self.path` is '/', the path
        # does not end with '/'. This function determines
        # whether a blob with this name exists.
        # If it does, it is equivalent to a *file*.
        # Note the difference between `_blob_exists`
        # and `exists`.
        raise NotImplementedError

    @abc.abstractmethod
    def recursive_iterdir(self: T) -> Iterator[T]:
        '''Yield blobs under the current "directory".

        For example, if full path is

            /ab/cd/efgh

        then yield blobs named like

            /ab/cd/efgh/j
            /ab/cd/efgh/k/p.txt
            /ab/cd/efgh/o/p/q.data

        However, do not yield blobs named like

            /ab/cd/efghij
            /ab/cd/efghx/y

        S3, Azure, GCP all have API's to list blobs
        whose name starts with a given prefix.
        In this case, the prefix should be essentially
        the fully qualitied name with '/' appended to the end.
        '''
        raise NotImplementedError

    async def a_download(self, *args, **kwargs):
        return await self._a_do(self.download, *args, **kwargs)

    async def a_upload(self, *args, **kwargs):
        return await self._a_do(self.upload, *args, **kwargs)

    def download(self,
                 target: Union[str, pathlib.Path, LocalUpath],
                 *,
                 exist_action: str = None) -> int:
        return self.copy_out(target, exist_action=exist_action)

    def exists(self):
        if self._blob_exists():
            return True
        if self.is_dir():
            return True
        return False

    def is_dir(self):
        '''In a typical blob store, there is no such concept as a
        "directory". Here we emulate the situation in a local file
        system. If there are blobs named like

            /ab/cd/ef/g.txt

        we say there exists directory "/ab/cd/ef".
        We should never have blobs named like

            /ab/cd/ef/

        (I don't know whether the blob store offerings allow
        such blob names.) Consequently, `is_dir` is equivalent
        to "have stuff in the dir". There is no such thing as
        an "empty directory" in blob stores.
        '''
        try:
            next(self.recursive_iterdir())
            return True
        except StopIteration:
            return None

    def is_file(self):
        if self._blob_exists():
            return True
        return None

    def iterdir(self):
        p0 = self._path
        if not p0.endswith('/'):
            p0 += '/'
        np0 = len(p0)
        subdirs = set()
        for p in self.recursive_iterdir():
            tail = p._path[np0:]
            if '/' in tail:
                sub = tail[: tail.find('/')]
                if sub not in subdirs:
                    yield self / sub
                    subdirs.add(sub)
            else:
                yield self / tail

    def mkdir(self, *, parents=False, exist_ok=False):
        if self.is_dir():
            if exist_ok:
                return
            raise FileExistsError(str(self))
        else:
            if not parents:
                if not self.parent.is_dir():
                    raise FileNotFoundError(str(self.parent))
            # There is no need to "create a directory"
            # in a blob store. Just go ahead creating
            # blobs under the "directory".

    def rmdir(self):
        if self.is_dir():
            raise FileExistsError(str(self))

    def upload(self,
               source: Union[str, pathlib.Path, LocalUpath],
               *,
               exist_action: str = None) -> int:
        return self.copy_in(source, exist_action=exist_action)
