from __future__ import annotations
# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import abc
import asyncio
import concurrent.futures
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
from typing import List, Iterator, TypeVar


logger = logging.getLogger(__name__)
T = TypeVar('T', bound='Upath')


class LockAcquisitionTimeoutError(TimeoutError):
    pass


class Upath(abc.ABC):  # pylint: disable=too-many-public-methods
    _executor: concurrent.futures.ThreadPoolExecutor = None

    def __init__(self, *pathsegments: str, **kwargs):
        '''`Upath` is the base class for a client to a blob store,
        including local file system as a special case.

        `*pathsegments`: analogous to the input to `pathlib.Path`.
        The first segment may or may not start with `/`; it makes
        no difference. The path constructed with `*pathsegments`
        is always "absolute" under a known "root".

        For a local POSIX file system, the root is the regular `/`.
        For Azure blob store, the root is that of a "container".
        For AWS and GCP blob stores, the root is that of a "bucket".

        `**kwargs`: additional arguments. This usually concerns
        credentials and top-level "divisions" for a blob store.
        (For example, "container" for Azure, "bucket" for AWS or GCP.)
        In general, these arguments should be read-only
        and remain unchanged during the lifetime of the object.
        For local file system, this is not needed (refer to class `LocalUpath`).
        '''

        self._path = os.path.normpath(os.path.join(
            '/', *pathsegments))  # pylint: disable=no-value-for-parameter
        # The path is always "absolute" starting with '/'.
        # Unless it is `/`, it does not have a trailing `/`.

        self._kwargs = kwargs
        # The extra `kwargs` is handled this way so that
        # the few functions that calls `self.__class__(...)`
        # work with subclasses that take additional arguments
        # in their `__init__`.
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

    def __truediv__(self: T, key: str) -> T:
        return self.joinpath(key)

    def copy_from(self,
                  source: Upath,
                  *,
                  concurrency: int = None,
                  exist_action: str = None) -> int:
        '''Copy the content of `source` into `self`.

        `source` may be a file or a directory.
        In the latter case, its content will be copied recursively.

        Return the number of files copied.
        '''
        return source.copy_to(self,
                              concurrency=concurrency,
                              exist_action=exist_action)

    def _copy_file(self, target: Upath, *, exist_action: str) -> int:
        if target == self:
            return 0

        if target.isfile():
            if exist_action == 'raise':
                raise FileExistsError(target)
            if exist_action == 'skip':
                logger.info(f"target {target!r} exists; skipped")
                return 0
            target.write_bytes(self.read_bytes(), overwrite=True)
            return 1

        if target.isdir():
            # Do not delete.
            raise FileExistsError(target)

        target.write_bytes(self.read_bytes(), overwrite=False)
        return 1

    def copy_to(self,
                target: Upath,
                *,
                concurrency: int = None,
                exist_action: str = None,
                ) -> int:
        '''Copy the content of `self` to the specified `target`,
        which is typically in another store.

        `concurrency`: number of threads to use. If `None`,
        a default value (e.g. 4) is used.

        `exist_action`: what to do when the target file already exists.
        There are three possible values:

            'raise' (default): raise `FileExistsError`.
            'skip': skip this file; proceed to work on other files.
            'overwrite': overwrite the existing file.

        The behavior is analogous to the command `cp` in Linux:

            # self   target  outcome
            abc.txt, xy      ==> xy
            abc.txt, xy/     => xy/abc.txt
            abc/,    xy      ==> xy/
            abc/,    xy/     ==> xy/abc/

        Return the number of files copied.

        Compare with `cp`, which copies to another location
        in the same store.

        Subclasses should provide more efficient implementations
        if possible, while maintains the behavior defined in
        this implementation.
        '''
        if exist_action is None:
            exist_action = 'raise'
        else:
            assert exist_action in ('raise', 'skip', 'overwrite')

        if target.isdir():
            target = target / self.name

        if self.isfile():
            return self._copy_file(target, exist_action=exist_action)

        if self.isdir():
            if concurrency is None:
                concurrency = 4
            else:
                assert 0 <= concurrency <= 16
            pool = concurrent.futures.ThreadPoolExecutor(concurrency)
            tasks = []
            for p in self.riterdir():
                extra = str(p.path.relative_to(self.path))
                tasks.append(pool.submit(
                    p._copy_file,
                    target=target/extra,
                    exist_action=exist_action
                ))
            n = 0
            for f in concurrent.futures.as_completed(tasks):
                n += f.result()
            return n

        raise FileNotFoundError(self)

    def cp(self: T,
           target: str,
           *,
           overwrite: bool = False,
           concurrency: int = None) -> T:
        '''Copy the content of the current path to the location
        `target` in the same store. The path `target` is relative
        to `self`.

        Return a path to the target.

        Examples: suppose these blobs are present

            /a/b/c.txt
            /a/b/c/d/c.txt
            /e/f/g/xy.data
            /e/f/g/h/d/dd.txt

        now with `overwrite=False`:

            self         target         outcome
            /a/b/c.txt   ../c           /a/b/c/c.txt
            /a/b/c.txt   ../c/d         FileExistsError
            /a/b/c.txt   ../c.txt.back  /a/b/c.txt.back
            /a/b/c/d     /e/f           /e/f/d/c.txt
            /a/b/c/d     /e/f/g/h       FileExistsError
        '''
        raise NotImplementedError

    @ abc.abstractmethod
    def exists(self) -> bool:
        '''Return `True` if the path is an existing file or dir,
        `False` otherwise.

        In a blobstore with blobs

            /a/b/cd
            /a/b/cd/e.txt

        '/a/b/cd' exists, and is both a file and a dir;
        '/a/b/cd/e.txt' exists, and is a file;
        '/a/b' exists, and is a dir;
        '/a/b/c' does not exist.
        '''
        raise NotImplementedError

    @ abc.abstractmethod
    def isdir(self) -> bool:
        '''Return `True` if the path is an existing directory, `False` otherwise.

        If there exists a file named like

            /a/b/c/d.txt

        we say `/a`, `/a/b`, `/a/b/c` are existing directories.

        In a cloud blob store, there's no such thing as an
        "empty directory", because there is no concept of "directory".
        A blob store just consists of files (aka blobs) with names,
        which could contain the letter '/', with no special meaning
        attached to it.
        We interpret the name `/a/b` as a directory
        to emulate the familiar concept in a local file system because
        there exist files named `/a/b/...`.

        In a local file system, there can be empty directories.
        However, the class `LocalUpath` tries to prevent that from happening
        (but can't strictly enforce it).
        Therefore, `isdir` can be understood as "is a non-empty dir".
        Consequently, there is usually no need to check whether a directory is
        "empty".

        Along similar lines, there is no method for "creating a dir" (like `mkdir`).
        Simply create a file under the dir, and the dir will come into being.
        This is analogous to our treatment to files---we don't "create" a file
        in advance; we simply write to a path, intending it to be a file.
        '''
        raise NotImplementedError

    @ abc.abstractmethod
    def isfile(self) -> bool:
        '''Return `True` if the path is an existing file, `False` otherwise.

        In a cloud blob store, a path can be both a file and a dir. For
        example, if these blobs exist:

            /a/b/c/d.txt
            /a/b/c

        we say `/a/b/c` is a "file", and also a "dir".
        User is recommended to avoid such namings.

        This situation does not happen in a local file system.'''
        raise NotImplementedError

    @ abc.abstractmethod
    def iterdir(self: T) -> Iterator[T]:
        '''Yield the first-level (i.e. non-recursive) children
        of the current dir.

        Each yielded element is either a file or a dir.

        If `self` is not a dir, or does not exist at all,
        yield nothing, but do not raise exception.

        There is no guarantee on the order of the returned elements.'''
        raise NotImplementedError

    def joinpath(self: T, *other: str) -> T:
        '''Join this path with more segments, return the new path object.'''
        return self.__class__(self._path, *other, **self._kwargs)

    @ contextlib.contextmanager
    @ abc.abstractmethod
    def lock(self, *, wait: float = 60):
        '''Lock the file pointed to, in order to have exclusive access.

        `wait`: if the lock can't be acquired within *wait* seconds,
        raise `LockAcquisitionTimeoutError`.

        This is a "mandatory lock", as opposed to an "advisory lock".
        However, this API does not specify that the locked file
        can be accessed for its content or used in any particular way.
        The intended use case is for this lock to be used
        in implementing a (cooperative) "code lock".

        The `yield` statement is not required to yield anything,
        that is, it may be simply

            yield

        rather than, say,

            yield self

        One way to achive cooperative locking on a file via this mandatory
        lock is like this:

            f = Upath('abc.txt')
            with f.with_suffix('.txt.lock').lock():
                ...  # now read/write `f` with exclusive access

        Some storage engines may not provide the capability to implement
        this lock.
        '''
        raise NotImplementedError

    def ls(self: T) -> List[T]:
        return sorted(self.iterdir())

    def mv(self: T, target: str, *, overwrite: bool = False) -> T:
        '''Rename this file or directory to the given `target`
        in the same store.

        Return a new `Upath` instance pointing to `target`.

        Behavior is analogous to the Linux command `mv`.
        Aslo refer to the doc of `cp`.

        This reference implementation uses copy/delete
        to achieve the effect of renaming.
        Concrete subclasses may have a more efficient way
        to achieve renaming.
        '''
        # TODO: needs more careful check on the location relationship
        # between `self` and `target`.
        raise NotImplementedError

    @ property
    def name(self) -> str:
        '''Return the segment after the last `/`.'''
        # If `self.path` is '/', then `self.path.name` is ''.
        return self.path.name

    @ property
    def parent(self: T) -> T:
        return self.__class__(str(self.path.parent), **self._kwargs)

    @ property
    def path(self) -> pathlib.PurePosixPath:
        return pathlib.PurePosixPath(self._path)

    @ abc.abstractmethod
    def read_bytes(self) -> bytes:
        '''Return the binary contents of the file.

        If `self` is not a file or is non-existent,
        raise `FileNotFoundError`.
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
    def riterdir(self: T) -> Iterator[T]:
        '''Yield files under the current dir recursively.

        Compared to `iterdir`, this is recursive, and yields
        *files* only. Empty subdirectories will have no representation
        in the return.

        Similar to `iterdir`, if `self` is not a dir or does not exist,
        then nothing is yielded, and no exception is raised either.

        There is no guarantee on the order of the returned elements.'''

        raise NotImplementedError

    @ abc.abstractmethod
    def rmdir(self, *, missing_ok: bool = False, concurrency: int = None) -> int:
        '''Remove the directory pointed to by `self`,
        along with all its contents, recursively.

        Return the number of files removed.

        `concurrency`: number of threads to use. If `None`,
        a default value (e.g. 4) is used.

        If `self.exists()` is `False` or `self.isdir()` is `False`,
        and `missing_ok` is `False`, raise `FileNotFoundError`;
        otherwise, return 0.
        '''
        raise NotImplementedError

    @ abc.abstractmethod
    def rmfile(self, *, missing_ok: bool = False) -> int:
        '''Remove the file pointed to by `self`.

        Return the number of files removed (0 or 1).

        If `self.exists()` is `False` or `self.isfile()` is `False`,
        and `missing_ok` is `False`, raise `FileNotFoundError`;
        otherwise, return 0.

        If the file is the only element in its parent directory,
        then the directory is also removed. This is to avoid having
        empty directories. (See doc of `isdir`.)
        '''
        raise NotImplementedError

    def rmrf(self, *, concurrency: int = None) -> int:
        '''Analogous to `rm -rf`. Remove the file or dir pointed to
        by `self`.

        Return the number of files removed.

        `concurrency`: number of threads to use. If `None`,
        a default value (e.g. 4) is used.

        For example, if these blobs are present:

            /a/b/c/d/e.txt
            /a/b/c/kk.data
            /a/b/c

        then `Upath('/a/b/c')` would remove all of them.
        '''
        if self._path == '/':
            raise UnsupportedOperation("`rmrf` not allowed on root directory")
        n1 = self.rmfile(missing_ok=True)
        n2 = self.rmdir(missing_ok=True, concurrency=concurrency)
        return n1 + n2

    @ abc.abstractmethod
    def stat(self) -> os.stat_result:
        # TODO: spec of the output content.
        raise NotImplementedError

    @ property
    def stem(self) -> str:
        return self.path.stem

    @ property
    def suffix(self) -> str:
        return self.path.suffix

    @ property
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

    @ abc.abstractmethod
    def write_bytes(self,
                    data: bytes,
                    *,
                    overwrite: bool = False) -> int:
        '''Write bytes to file.

        Return number of bytes written.

        Parent directories are created as needed.

        `overwrite`: overwrite existing file?
            If `False`, and file exists, raises `FileExistsError`.
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
                   encoding: str = 'utf-8',
                   errors: str = 'strict',
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

    async def a_copy_from(self, *args, **kwargs):
        return await self._a_do(self.copy_from, *args, **kwargs)

    async def a_copy_to(self, *args, **kwargs):
        return await self._a_do(self.copy_to, *args, **kwargs)

    async def a_cp(self, *args, **kwargs):
        return await self._a_do(self.cp, *args, **kwargs)

    async def a_exists(self):
        return await self._a_do(self.exists)

    async def a_isdir(self):
        return await self._a_do(self.isdir)

    async def a_isfile(self):
        return await self._a_do(self.isfile)

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

    async def a_ls(self):
        return await self._a_do(self.ls)

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

    async def a_riterdir(self):
        # This is a suboptimal reference implementation.
        for p in self.riterdir():
            yield p

    async def a_rmdir(self, *args, **kwargs):
        return await self._a_do(self.rmdir, *args, **kwargs)

    async def a_rmfile(self, *args, **kwargs):
        return await self._a_do(self.rmfile, *args, **kwargs)

    async def a_rmrf(self, *args, **kwargs):
        return await self._a_do(self.rmrf), *args, **kwargs

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
