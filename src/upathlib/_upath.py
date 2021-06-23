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
from typing import List, Union, Iterator, TypeVar, Optional

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


class LockAcquisitionTimeoutError(TimeoutError):
    pass


class Upath(abc.ABC):  # pylint: disable=too-many-public-methods
    _executor: ThreadPoolExecutor = None

    def __init__(self, *parts: str, **kwargs):
        self._path = os.path.normpath(os.path.join(
            '/', *parts))  # pylint: disable=no-value-for-parameter
        # The path is always "absolute" starting with '/'.
        # Unless it is `/`, it does not have a trailing `/`.
        self._kwargs = kwargs

        # The extra `kwargs` is handled this way so that
        # the few functions that calls `self.__class__(...)`
        # work with subclasses that take additional arguments
        # in their `__init__`.
        # In general, these arguments should be read-only
        # and remain unchanged during the lifetime of the object.
        # A subclass that uses such extra arguments may need to
        # redefine the "comparison" special methods to take
        # into account some of these parameters as needed.

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

    def __copy__(self: T) -> T:
        return self.__class__(self._path, **self._kwargs)

    def __hash__(self) -> int:
        return hash((
            self.__class__.__name__,
            self._path,
            tuple(self._kwargs.items())
        ))
        # TODO: with some unusual values of `self._kwargs`,
        # this could be unhashable.

    def __truediv__(self: T, key: str) -> T:
        return self.joinpath(key)

    def clear(self) -> None:
        '''Clear all contents of the directory, but keep the directory.

        If the path is not a directory, raise NotADirectoryError.
        '''
        for p in self.iterdir():
            p.rmrf()

    def copy_from(self,
                  source: Union[str, pathlib.Path, Upath],
                  *,
                  exist_action: str = None) -> int:
        '''This is like "import" or "upload".
        The reverse of `copy_to`.

        Return the number of files copied.

        The name 'import' can't be used. The name 'upload'
        would be confusing when we do something like

            local_upath.upload(remote_upath)

        which by the design of this method would mean
        copying from `remote_upath` to `local_upath`.
        '''
        if isinstance(source, str):
            source = pathlib.Path(source)
        if isinstance(source, pathlib.Path):
            source = LocalUpath(str(source.absolute()))
        else:
            assert isinstance(source, Upath)
        return source.copy_to(self, exist_action=exist_action)

    def _copy_to_internal(self, target: Upath, *, exist_action: str) -> int:
        if target == self:
            return 0

        if self.is_file():
            if target.is_file():
                if exist_action == 'raise':
                    raise FileExistsError(target)
                if exist_action == 'skip':
                    logger.info(f"target {target!r} exists; skipped")
                    return 0
                target.write_bytes(self.read_bytes(), overwrite=True)
                return 1
            if target.is_dir():
                # Do not delete.
                raise FileExistsError(target)
            target.write_bytes(self.read_bytes(), overwrite=True)
            return 1

        if not self.is_dir():
            logger.info(
                f"source {self!r} is neither file nor directory; skipped")
            return 0

        if target.is_file():
            if exist_action == 'raise':
                raise FileExistsError(target)
            if exist_action == 'skip':
                logger.info(f"target {target!r} exists; skipped")
                return 0
            target.rm()

        # If `target` is an existing directory, just copy
        # into it. If `target` contains files that are not present
        # in the source, those files are untouched.

        n = 0
        for s in self.iterdir():
            k = s._copy_to_internal(target / s.name,
                                    exist_action=exist_action)
            n += k

        return n

    # TODO: use multiple threads.
    def copy_to(self,
                target: Union[str, pathlib.Path, Upath],
                *,
                exist_action: str = None) -> int:
        '''Copy the content of the `self` path to the specified `target`
        in another store. Return number of files copied.

        Compare with `cp`, which copies to another location in the
        same store.

        `exist_action`: what to do when the target file already exists.
        There are three acceptible values:

            'raise' (default): raise FileExistsError.
            'skip': skip this file; proceed to work on other files.
            'overwrite': overwrite the existing target file.

        Subclasses should provide more efficient implementations
        if possible, while maintains the behavior defined in
        this implementation.

        The behavior is analogous to the command `cp` in Linux:

            abc.txt, xy ==> xy
            abc.txt, xy/ => xy/abc.txt
            abc/, xy ==> xy/
            abc/, xy/ ==> xy/abc/
        '''
        if not self.exists():
            raise FileNotFoundError(self)

        if isinstance(target, str):
            target = pathlib.Path(target)
        if isinstance(target, pathlib.Path):
            target = LocalUpath(target.absolute())
        else:
            assert isinstance(target, Upath)

        if target == self:
            return 0

        if exist_action is None:
            exist_action = 'raise'
        else:
            assert exist_action in ('raise', 'skip', 'overwrite')

        if target.is_dir():
            target = target / self.name

        return self._copy_to_internal(target, exist_action=exist_action)

    def cp(self: T, target: str, exist_action: str = None) -> T:
        '''Copy the content of the current path to the location
        `target` in the same store.'''
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

        In practice, one probably should avoid that situation
        where a path is both a file and a dir.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def is_dir(self) -> Optional[bool]:
        '''Return `True` if the path is an existing directory,
        `False` if an existing non-directory, `None` if non-existent.'''
        raise NotImplementedError

    def is_empty_dir(self) -> Optional[bool]:
        if not self.exists():
            return None
        if not self.is_dir():
            return False
        try:
            _ = next(self.iterdir())
            return False
        except StopIteration:
            return True

    @abc.abstractmethod
    def is_file(self) -> Optional[bool]:
        '''Return `True` if the path is an existing file,
        `False` if an existing non-file, `None` if non-existent.'''
        raise NotImplementedError

    @abc.abstractmethod
    def iterdir(self: T) -> Iterator[T]:
        '''When the path points to a directory, yield path objects of the
        directory contents. Only one level down; not recursively.'''
        raise NotImplementedError

    def joinpath(self: T, *other: str) -> T:
        '''Join this path with more segments, return the new path object.'''
        return self.__class__(self._path, *other, **self._kwargs)

    @contextlib.contextmanager
    @abc.abstractmethod
    def lock(self, *, wait: float = 60):
        '''Lock the file pointed to, in order to have exclusive access.

        `wait`: if the lock can't be acquired within *wait* seconds,
        raise `LockAcquisitionTimeoutError`.

        This is a "mandatory lock", as opposed to an "advisory lock".
        However, this API does not specify that the locked file
        can be used for its content. (A subclass may provide that capability
        if it so wishes.) The design use case is for this lock
        to be used in implementing a (cooperative) "code lock".

        The `yield` statement is not required to yield anything in particular,
        that is, it may be simply

            yield

        rather than, say,

            yield self

        One way to achive cooperative locking on a file via this mandatory
        lock is like this:

            f = Upath('abc.txt')
            with f.with_suffix('.txt.lock').lock():
                ...  # use `f` with exclusive access

        Some storage engines may not provide the capability to implement
        this lock.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def mkdir(self: T, *, exist_ok: bool = False) -> T:
        '''Create a new directory at this given path. Return self.

        If the directory already exists, and `exist_ok` is False,
        raise FileExistsError.'''
        raise NotImplementedError

    def _mv_internal(self: T, target: Upath, *, overwrite: bool) -> T:
        if self.is_file():
            if target.is_file():
                if not overwrite:
                    raise FileExistsError(target)
                target.write_bytes(self.read_bytes(), overwrite=True)
            elif target.is_dir():
                raise FileExistsError(target)
            else:
                target.write_bytes(self.read_bytes(), overwrite=True)
            self.rm()
            return target

        if self.is_dir():
            if target.exists():
                if not overwrite:
                    raise FileExistsError(target)
                target.rmrf()
            for p in self.iterdir():
                p._mv_internal(target / p.name)
            self.rmdir()
            return target

        raise RuntimeError('should never reach here')

    def mv(self: T, target: str, *, overwrite: bool = False) -> T:
        '''Rename this file or directory to the given `target`
        in the same store, and
        return a new Upath instance pointing to `target`.

        Behavior is analogous to the Linux command `mv`.

        This reference implementation uses copy/delete
        to achieve the effect of renaming.
        Concrete subclasses may have a more efficient way
        to conduct renaming.
        '''
        # TODO: needs more careful check on the location relationship
        # between `self` and `target`.
        target = self / target
        if target.is_dir():
            target = target / self.name

        if target == self:
            return self
        if not self.exists():
            raise FileNotFoundError(self)

        return self._mv_internal(target, overwrite=overwrite)

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
        '''Return the binary contents of the file.

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
        '''Removes the file pointed to by `self`.
        Return number of files removed.

        If the file does not exist, and `missing_ok` is False,
        raise FileNotFoundError.

        If `self` is a directory, raise IsADirectoryError.
        In this case, use `rmdir` instead.
        '''
        raise NotImplementedError

    @abc.abstractmethod
    def rmdir(self) -> None:
        '''Remove the directory pointed to by `self`.
        The directory must be empty.

        If the directory is not empty, raise OSError.'''
        raise NotImplementedError

    def rmrf(self) -> int:
        '''Analogous to `rm -rf`. Return number of files removed.

        The object pointed to by `self` may be either a file
        or a directory. If file, remove it. If directory,
        remove its contents recursively, and finally the directory itself.

        Compare `rmrf` with `clear`. The latter removes content
        in the directory but keeps the directory itself.
        Also, `clear` does not work on a file.
        '''
        if self._path == '/':
            raise UnsupportedOperation("`rmrf` not allowed on root directory")

        k = 0
        if self.is_file():
            logger.info(f'deleting {self}')
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
        # TODO: spec of the output content.
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
        '''`suffix` should include a dot, like '.txt'.
        If `suffix` is '', the effect is to remove the existing suffix.
        '''
        return self.__class__(str(self.path.with_suffix(suffix)),
                              **self._kwargs)

    @abc.abstractmethod
    def write_bytes(self,
                    data: bytes,
                    *,
                    overwrite: bool = False) -> int:
        '''Write bytes to file. Parent directories are created as needed.

        Return number of bytes written.

        `overwrite`: overwrite existing file?
            If False, and file exists, raises FileExistsError.

        If the object pointed to is a directory, raise IsADirectoryError.
        '''
        raise NotImplementedError

    def write_json(self, data, *, overwrite=False, **kwargs) -> int:
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

    async def _a_do(self, func, *args, **kwargs):
        func = partial(func, *args, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, func)

    async def a_clear(self, *args, **kwargs):
        return await self._a_do(self.clear, *args, **kwargs)

    async def a_copy_from(self, *args, **kwargs):
        return await self._a_do(self.copy_from, *args, **kwargs)

    async def a_copy_to(self, *args, **kwargs):
        return await self._a_do(self.copy_to, *args, **kwargs)

    async def a_cp(self, *args, **kwargs):
        return await self._a_do(self.cp, *args, **kwargs)

    async def a_exists(self):
        return await self._a_do(self.exists)

    async def a_is_dir(self):
        return await self._a_do(self.is_dir)

    async def a_is_empty_dir(self):
        return await self._a_do(self.is_empty_dir)

    async def a_is_file(self):
        return await self._a_do(self.is_file)

    async def a_iterdir(self):
        # This is a suboptimal reference implementation.
        for p in self.iterdir():
            yield p

    @contextlib.asynccontextmanager
    async def a_lock(self, *, wait: float = 60):
        # This implementation may be suboptimal.
        # Subclass should provide a better implementation
        # if available.
        with self.lock(wait=wait) as obj:
            yield obj

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

    def mkdir(self, *, exist_ok=False):
        self.localpath.mkdir(parents=True, exist_ok=exist_ok)
        return self

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
        self.localpath.rmdir()

    def stat(self):
        return self.localpath.stat()

    def write_bytes(self, data: bytes, *, overwrite=False):
        if self.is_file():
            if not overwrite:
                raise FileExistsError(self)
        else:
            self.parent.mkdir(exist_ok=True)
        return self.localpath.write_bytes(data)


class BlobUpath(Upath):  # pylint: disable=abstract-method
    def __init__(self, *parts: str, **kwargs):
        super().__init__(*parts, **kwargs)
        self._as_dir = None
        if self._path == '/':
            self._as_dir = True
        else:
            if parts:
                if parts[-1].endswith('/'):
                    self._as_dir = True

    @abc.abstractmethod
    def _blob_exists(self) -> bool:
        # Unless `self.path` is '/', the path
        # does not end with '/'. This function determines
        # whether a blob with this name exists.
        # If it does, it is equivalent to a *file*.
        # Note the difference between `_blob_exists`
        # and `exists`.
        raise NotImplementedError

    def clear(self):
        n = 0
        for p in self._recursive_iterdir():
            p.rm()
            n += 1
        if n > 0:
            self._as_dir = True

    def download(self,
                 target: Union[str, pathlib.Path, LocalUpath],
                 *,
                 exist_action: str = None) -> int:
        return self.copy_to(target, exist_action=exist_action)

    def exists(self):
        if self._blob_exists():
            return True
        try:
            next(self._recursive_iterdir())
            return True
        except StopIteration:
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
        such blob names.)

        Consequently, `is_dir` is almost equivalent
        to "having stuff in the dir". There is no such thing as
        an "empty directory" in blob stores.
        However, we provide two ways to emulate an "empty dir".
        The first way is a call to `mkdir`. The second way is
        to include a trailing '/' in the name, as in

            BlobUpath('ab', 'cd', 'efg/')
            blob_upath / 'xy/'

        Both ways mark the path as a dir in the remaining life
        of the BlobUpath object. The mark is not persisted anywhere
        outside of the object. Given the subtlety involved, this
        feature is not highlighted for now.
        '''
        if self._as_dir:
            return True
        try:
            next(self._recursive_iterdir())
            return True
        except StopIteration:
            if self._blob_exists():
                return False
            return None

    def is_file(self):
        if self._blob_exists():
            return True
        return None

    def iterdir(self):
        # For efficiency reasons, this does not first check that
        # `self` is a dir, and raise NotADirectoryError if it isn't.
        # This could change later, to be aligned with the behavior of
        # `LocalUpath` as well as `pathlib`.
        p0 = self._path  # this could be '/'.
        if not p0.endswith('/'):
            p0 += '/'
        np0 = len(p0)
        subdirs = set()
        for p in self._recursive_iterdir():
            tail = p._path[np0:]
            if '/' in tail:
                tail = tail[: tail.find('/')]
            if tail not in subdirs:
                yield self / tail
                subdirs.add(tail)

    def mkdir(self, *, exist_ok=False):
        if self.is_dir():
            if exist_ok or self.is_empty_dir():
                return self
            raise FileExistsError(self)
        else:
            # Make sure that a path name can't be both a file
            # and a directory.
            p = self
            while p._path != '/':
                if p.is_file():
                    raise FileExistsError(p)
                p = p.parent

            self._as_dir = True
            # There is no need to "create a directory"
            # in a blob store. Just go ahead creating
            # blobs under the "directory".
            return self

    @abc.abstractmethod
    def _recursive_iterdir(self: T) -> Iterator[T]:
        '''Yield blobs under the current "directory".

        For example, if self._path is

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
        `self._path` with '/' appended to the end.

        This classes lists `_recursive_iterdir` as abstract,
        while `iterdir` is implemented using `_recursive_iterdir`.
        A concrete subclass may choose to implement `_recursive_iterdir`
        (leaving `iterdir` to the implementation provided in this class),
        or `iterdir` (and `_recursive_iterdir` implemented using `iterdir`),
        or both `iterdir` and `_recrusive_iterdir` separately, depending
        on the capabilities of the API of the storage engine.
        '''
        raise NotImplementedError

    def rmdir(self):
        try:
            next(self._recursive_iterdir())
            raise FileExistsError(self)
        except StopIteration:
            self._as_dir = False

    def upload(self,
               source: Union[str, pathlib.Path, LocalUpath],
               *,
               exist_action: str = None) -> int:
        return self.copy_from(source, exist_action=exist_action)

    @abc.abstractmethod
    def write_bytes(self, data, *, overwrite=False):
        # Make sure that a path name can't be both a file
        # and a directory.
        # TODO: this logic makes too many service calls.
        if self.is_dir():
            raise IsADirectoryError(self)
        if self._path == '/':
            raise IsADirectoryError(self)
        p = self.parent
        while p._path != '/':
            if p.is_file():
                raise FileExistsError(p)
            p = p.parent

        if self.is_file():
            if overwrite:
                self.rm()
            else:
                raise FileExistsError(self)
        # subclass implementation should pick up here.

    async def a_download(self, *args, **kwargs):
        return await self._a_do(self.download, *args, **kwargs)

    async def a_upload(self, *args, **kwargs):
        return await self._a_do(self.upload, *args, **kwargs)
