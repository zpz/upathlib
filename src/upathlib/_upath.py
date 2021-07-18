from __future__ import annotations
# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import abc
import asyncio
import concurrent.futures
import contextlib
import datetime
import gc
import json
import logging
import os
import os.path
import pathlib
import pickle
from dataclasses import dataclass
from functools import partial
from io import UnsupportedOperation
from typing import List, Iterator, TypeVar, Any, Optional


logger = logging.getLogger(__name__)
T = TypeVar('T', bound='Upath')


class LockAcquisitionTimeoutError(TimeoutError):
    pass


@dataclass
class FileInfo:
    ctime: float   # creation POSIX timetamp
    mtime: float   # last modification POSIX timestamp
    time_created: datetime.datetime
    time_modified: datetime.datetime
    size: int      # in bytes
    details: Any   # platform-dependent


def nogc(func, *args, **kwargs):
    isgc = gc.isenabled()
    if isgc:
        gc.disable()
    try:
        return func(*args, **kwargs)
    finally:
        if isgc:
            gc.enable()


def _execute_in_thread_pool(jobs, concurrency: int = None):
    if concurrency is None:
        concurrency = 4
    else:
        assert 0 <= concurrency <= 16
        if concurrency < 1:
            concurrency = 1

    pool = concurrent.futures.ThreadPoolExecutor(concurrency)
    tasks = []
    for f, args, kwargs in jobs:
        tasks.append(pool.submit(f, *args, **kwargs))
    results = []
    for f in concurrent.futures.as_completed(tasks):
        results.append(f.result())
    return results


class Upath(abc.ABC):  # pylint: disable=too-many-public-methods

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

    def __hash__(self) -> int:
        return hash(repr(self))

    def __truediv__(self: T, key: str) -> T:
        return self.joinpath(key)

    def copy_dir(self: T, target: str, *, overwrite: bool = False, concurrency: int = None) -> T:
        '''Analogous to `copy_file`.
        '''
        target = self.parent / target

        def foo():
            for p in self.riterdir():
                extra = str(p.path.relative_to(self.path))
                yield (
                    p.copy_file,
                    [(target / extra)._path],
                    {'overwrite': overwrite},
                )

        results = _execute_in_thread_pool(foo(), concurrency)
        n = len(results)
        # TODO: get the actual number of files copied.

        if n == 0:
            raise FileNotFoundError(self)
        logger.info('%d files copied', n)
        return target

    @abc.abstractmethod
    def copy_file(self: T, target: str, *, overwrite: bool = False) -> T:
        '''Copy file to `target` in the same store.

        `target` is either absolute, orrelative to `self.parent`.
        For example, if `self` is '/a/b/c/d.txt', then
        `target='e.txt'` means '/a/b/c/e.txt'.

        If `self` is not an existing file, raise `FileNotFoundError`.

        If `target` is an existing file, then it is overwritten
        if `overwrite` is `True`, otherwise raise `FileExistsError`.

        If `target` is an existing directory, raise `FileExistsError`.
        Note: this behavior is different from the Unix command `cp`
        in this situation---it does not *copy into* the target directory.

        Return a `Upath` object pointing to `target`.
        '''
        # Reference implementation.
        # Subclass should implement by direct file operation if possible.
        target = self.parent / target
        target.write_bytes(self.read_bytes(), overwrite=overwrite)
        return target

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
        return self.is_file() or self.is_dir()

    def export_dir(self,
                   target: Upath,
                   *,
                   concurrency: int = None,
                   exist_action: str = None,
                   ) -> int:
        '''Copy the content of `self` to the specified `target`,
        which is typically in another store.

        Compare with `copy_dir`, which make copies within the same store.

        `concurrency`: number of threads to use. If `None`,
        a default value (e.g. 4) is used.

        Overwriting happens file-wise. For example, if the target directory
        contains files that do not exist in the source directory, they
        are left untouched.

        Return the number of files copied.
        '''
        def foo():
            for p in self.riterdir():
                extra = str(p.path.relative_to(self.path))
                yield (
                    p.export_file,
                    [],
                    {'target': target/extra, 'exist_action': exist_action},
                )

        nn = _execute_in_thread_pool(foo(), concurrency)
        n = sum(nn)
        if n:
            logger.info('%d files copied from %r to %r', n, self, target)
        else:
            logger.warning('%d files copied from %r to %r', n, self, target)
        return n

    def _should_update(self, other: Upath) -> bool:
        sourceinfo = self.file_info()
        targetinfo = other.file_info()
        return (sourceinfo.size != targetinfo.size
                or sourceinfo.mtime > targetinfo.mtime)
        # Otherwise, we're assuming that
        # the target file was copied from the source
        # previously.

    def _export_file(self, target: Upath, *, overwrite: bool = False) -> None:
        # Reference implementation.
        # Subclass may customize this to perform file download
        # when `target` is a `LocalUpath`.
        target.write_bytes(
            self.read_bytes(), overwrite=overwrite
        )

    def export_file(self, target: Upath, *, exist_action: str = None) -> int:
        '''Copy the file to the specified `target`, which is typically
        in another store.

        Return the number of files copied (0 or 1).

        `exist_action`: what to do when the target file already exists.
        These are the possible values:

            'raise' (default): raise `FileExistsError`.
            'skip': skip this file; proceed to work on other files.
            'overwrite': overwrite the existing file.
            'update': overwrite if source `mtime` is newer than target,
                or source and target have diff size; otherwise skip.

        The `target` specifies the name corresponding the the name of `self`.
        If `target` is an existing directory, a `FileExistsError` is raised.
        A copy is not placed *into* the target directory. This behavior
        differs from the Linux command `cp`.

        Compare with `copy_file`, which make copies within the same store.
        '''
        if not self.is_file():
            raise FileNotFoundError(self)

        if target.is_file():
            if exist_action is None:
                exist_action = 'raise'
            else:
                assert exist_action in ('raise', 'skip', 'overwrite', 'update')

            if exist_action == 'raise':
                raise FileExistsError(target)
            if exist_action == 'skip':
                logger.info(f"target {target!r} exists; skipped")
                return 0
            if exist_action == 'update':
                if not self._should_update(target):
                    logger.info(
                        f"target {target!r} appears to be up-to-date; skipped")
                    return 0
            logger.info("copying '%s' to '%s'", self, target)
            self._export_file(target, overwrite=True)
            return 1

        if target.is_dir():
            # Do not delete.
            raise FileExistsError(target)

        logger.info("copying '%s' to '%s'", self, target)
        self._export_file(target, overwrite=False)
        return 1

    @ abc.abstractmethod
    def file_info(self) -> Optional[FileInfo]:
        '''
        If `self.is_file()` is `False`, return `None`.
        '''
        raise NotImplementedError

    def import_dir(self,
                   source: Upath,
                   *,
                   concurrency: int = None,
                   exist_action: str = None,
                   ) -> int:
        '''Analogous to `export_dir`.
        '''
        return source.export_dir(self,
                                 concurrency=concurrency,
                                 exist_action=exist_action)

    def _import_file(self, source: Upath, *, overwrite: bool = False) -> None:
        # Subclass may customize this to perform file upload
        # when `target` is a `LocalUpath`.
        # This is not used by `import_file` directly, but
        # it is used by `export_file` in certain situations.
        # See `LocalUpath._export_file`.
        self.write_bytes(
            source.read_bytes(), overwrite=overwrite
        )

    def import_file(self, source: Upath, *, exist_action: str = None) -> int:
        return source.export_file(self, exist_action=exist_action)

    @ abc.abstractmethod
    def is_dir(self) -> bool:
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
        However, it is recommended to not have empty directories.

        There is no method for "creating a dir" (like `mkdir`).
        Simply create a file under the dir, and the dir will come into being.
        This is analogous to our treatment to files---we don't "create" a file
        in advance; we simply write to a path, intending it to be a file.
        '''
        raise NotImplementedError

    @ abc.abstractmethod
    def is_file(self) -> bool:
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

    def read_json(self, *, no_gc: bool = True, **kwargs):
        z = self.read_text(**kwargs)
        if no_gc:
            return nogc(json.loads, z)
        return json.loads(z)

    def read_pickle(self, *, no_gc: bool = True):
        z = self.read_bytes()
        if no_gc:
            return nogc(pickle.loads, z)
        return pickle.loads(z)

    def read_text(self, *, encoding: str = 'utf-8', errors: str = 'strict'):
        # Refer to https://docs.python.org/3/library/functions.html#open
        return self.read_bytes().decode(encoding=encoding, errors=errors)

    def remove_dir(self, *, missing_ok: bool = False, concurrency: int = None) -> int:
        '''Remove the directory pointed to by `self`,
        along with all its contents, recursively.

        Return the number of files removed.

        `concurrency`: number of threads to use. If `None`,
        a default value (e.g. 4) is used.

        If `self.exists()` is `False` or `self.is_dir()` is `False`,
        and `missing_ok` is `False`, raise `FileNotFoundError`;
        otherwise, return 0.

        Local upath needs to customize this implementation, because
        it needs to take care of deleting "empty" subdirectories.
        '''
        def foo():
            for p in self.riterdir():
                yield p.remove_file, [], {'missing_ok': False}

        nn = _execute_in_thread_pool(foo(), concurrency)
        n = sum(nn)
        if n == 0 and not missing_ok:
            raise FileNotFoundError(self)
        return n

    @ abc.abstractmethod
    def remove_file(self, *, missing_ok: bool = False) -> int:
        '''Remove the file pointed to by `self`.

        Return the number of files removed (0 or 1).

        If `self` is not an existing file, then raise `FileNotFoundError`
        if `missing_ok` is `False`, or return 0 otherwise.
        '''
        raise NotImplementedError

    def rename_dir(self: T, target: str, *, overwrite: bool = False, concurrency: int = None) -> T:
        '''Analogous to `rename_file`.

        If `self` is not an existing directory, raise `FileNotFoundError`.

        Local upath needs to customize this implementation, because
        it needs to take care to delete empty subdirectories under `self`.
        '''
        target = self.parent / target

        def foo():
            for p in self.riterdir():
                extra = str(p.path.relative_to(self.path))
                yield (
                    p.rename_file,
                    [(target / extra)._path],
                    {'overwrite': overwrite},
                )

        results = _execute_in_thread_pool(foo(), concurrency)
        n = len(results)
        if n == 0:
            raise FileNotFoundError(self)
        return target

    def rename_file(self: T, target: str, *, overwrite: bool = False) -> T:
        '''Rename the current file to `target` in the same store.

        `target` is relative to `self.parent`. For example, if `self`
        is '/a/b/c/d.txt', then `target='e.txt'` means '/a/b/c/e.txt'.

        Return an object pointing to the new path.

        If `self` is not an existing file, raise `FileNotFoundError`.

        This is a reference implementation that achieves renaming via
        copy-and-delete. In cloud blob stores, this could be the best
        we can do. For local file system, a more efficient approach should
        be used.
        '''
        t = self.copy_file(target, overwrite=overwrite)
        if t != self:
            self.remove_file()
        return t

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
        n1 = self.remove_file(missing_ok=True)
        n2 = self.remove_dir(missing_ok=True, concurrency=concurrency)
        return n1 + n2

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

    # async def _a_do(self, func, *args, **kwargs):
    #     func = partial(func, *args, **kwargs)
    #     return await asyncio.get_running_loop().run_in_executor(
    #         self._executor, func)

    # async def a_copy_dir(self, target, *, overwrite=False, concurrency: int = None):
    #     n = 0
    #     async for p in self.a_riterdir():
    #         extra = str(p.path.relative_to(self.path))
    #         await p.a_copy_file(os.path.join(target, extra), overwrite=overwrite)
    #         n += 1
    #     if n == 0:
    #         raise FileNotFoundError(self)
    #     logger.info('%d files copied', n)
    #     return self / target

    # async def a_copy_file(self, target, **kwargs):
    #     return await self._a_do(self.copy_file, target, **kwargs)

    # async def a_exists(self):
    #     return (await self.a_is_file()) or (await self.a_is_dir())

    # async def a_export_dir(self,
    #                        target: Upath,
    #                        *,
    #                        concurrency: int = None,
    #                        exist_action: str = None,
    #                        ) -> int:
    #     if concurrency is None:
    #         concurrency = 4
    #     else:
    #         assert 0 <= concurrency <= 16
    #         if concurrency < 1:
    #             concurrency = 1

    #     async def foo(source, target, exist_action, sem):
    #         async with sem:
    #             return await source.a_export_file(
    #                 target, exist_action=exist_action)

    #     sem = asyncio.Semaphore(concurrency)
    #     tasks = []
    #     async for p in self.a_riterdir():
    #         extra = str(p.path.relative_to(self.path))
    #         tasks.append(foo(
    #             p, target/extra,
    #             exist_action=exist_action, sem=sem))
    #     nn = await asyncio.gather(*tasks)
    #     n = sum(nn)
    #     if n:
    #         logger.info('%d files exported from %r to %r', n, self, target)
    #     else:
    #         logger.warning('%d files exported from %r to %r', n, self, target)
    #     return n

    # async def a_export_file(self, target: Upath, *, exist_action: str = None) -> int:
    #     if not await self.a_is_file():
    #         raise FileNotFoundError(self)

    #     if await target.a_is_file():
    #         if exist_action is None:
    #             exist_action = 'raise'
    #         else:
    #             assert exist_action in ('raise', 'skip', 'overwrite', 'update')

    #         if exist_action == 'raise':
    #             raise FileExistsError(target)
    #         if exist_action == 'skip':
    #             logger.info(f"target {target!r} exists; skipped")
    #             return 0
    #         if exist_action == 'update':
    #             sourceinfo = await self.a_file_info()
    #             targetinfo = await target.a_file_info()
    #             if (targetinfo.size == sourceinfo.size
    #                     and targetinfo.mtime >= sourceinfo.mtime):
    #                 # We're assuming that this suggests
    #                 # the target file was copied from the source
    #                 # previously.
    #                 logger.info(
    #                     f"target {target!r} appears to be up-to-date; skipped")
    #                 return 0
    #         logger.info("copying '%s' to '%s'", self, target)
    #         await self._a_export_file(target, overwrite=True)
    #         return 1

    #     if await target.a_is_dir():
    #         # Do not delete.
    #         raise FileExistsError(target)

    #     logger.info("copying '%s' to '%s'", self, target)
    #     await self._a_export_file(target, overwrite=False)
    #     return 1

    # async def _a_export_file(self, target: Upath, *, overwrite=False) -> None:
    #     await target.a_write_bytes(
    #         await self.a_read_bytes(), overwrite=overwrite
    #     )

    # async def a_file_info(self):
    #     return await self._a_do(self.file_info)

    # async def a_import_dir(self, source: Upath, *,
    #                        concurrency: int = None,
    #                        exist_action: str = None):
    #     return await source.a_export_dir(self,
    #                                      concurrency=concurrency,
    #                                      exist_action=exist_action,
    #                                      )

    # async def a_import_file(self, source: Upath, *, exist_action=None):
    #     return await source.a_export_file(self, exist_action=exist_action)

    # async def _a_import_file(self, source: Upath, *, overwrite=False) -> None:
    #     await self.a_write_bytes(
    #         await source.a_read_bytes(), overwrite=overwrite,
    #     )

    # async def a_is_dir(self):
    #     return await self._a_do(self.is_dir)

    # async def a_is_file(self):
    #     return await self._a_do(self.is_file)

    async def a_iterdir(self: T) -> Iterator[T]:
        # TODO: may need reimplementation.
        for p in self.iterdir():
            yield p

    # @ contextlib.asynccontextmanager
    # async def a_lock(self, *, wait: float = 60):
    #     # TODO: may need reimplementation.
    #     with self.lock(wait=wait):
    #         yield

    # async def a_ls(self):
    #     pp = [p async for p in self.a_iterdir()]
    #     return sorted(pp)

    # async def a_read_bytes(self):
    #     return await self._a_do(self.read_bytes)

    # async def a_read_json(self, *, no_gc: bool = True, **kwargs):
    #     z = await self.a_read_text(**kwargs)
    #     if no_gc:
    #         return nogc(json.loads, z)
    #     return json.loads(z)

    # async def a_read_pickle(self, *, no_gc: bool = True):
    #     z = await self.a_read_bytes()
    #     if no_gc:
    #         return nogc(pickle.loads, z)
    #     return pickle.loads(z)

    # async def a_read_text(self, *,
    #                       encoding: str = 'utf-8', errors: str = 'strict'):
    #     return (await self.a_read_bytes()).decode(
    #         encoding=encoding, errors=errors)

    # async def a_remove_dir(self, *,
    #                        missing_ok: bool = False, concurrency: int = None) -> int:
    #     # TODO: may need reimplementation.
    #     return await self._a_do(self.remove_dir,
    #                             missing_ok=missing_ok,
    #                             concurrency=concurrency)

    # async def a_remove_file(self, *args, **kwargs):
    #     return await self._a_do(self.remove_file, *args, **kwargs)

    # async def a_rename_dir(self, target, **kwargs):
    #     return await self._a_do(self.rename_dir, target, **kwargs)

    # async def a_rename_file(self, target, **kwargs):
    #     return await self._a_do(self.rename_file, target, **kwargs)

    async def a_riterdir(self: T) -> Iterator[T]:
        # TODO: may need reimplementation.
        for p in self.riterdir():
            yield p

    # async def a_rmrf(self, *, concurrency: int = None) -> int:
    #     if self._path == '/':
    #         raise UnsupportedOperation(
    #             "`a_rmrf` not allowed on root directory")
    #     n1 = await self.a_remove_file(missing_ok=True)
    #     n2 = await self.a_remove_dir(missing_ok=True, concurrency=concurrency)
    #     return n1 + n2

    # async def a_write_bytes(self, *args, **kwargs):
    #     return await self._a_do(self.write_bytes, *args, **kwargs)

    # async def a_write_json(self, data, *, overwrite=False, **kwargs) -> int:
    #     return await self.a_write_text(
    #         json.dumps(data), overwrite=overwrite, **kwargs)

    # async def a_write_pickle(self, data, *, overwrite=False) -> int:
    #     return await self.a_write_bytes(
    #         pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL),
    #         overwrite=overwrite,
    #     )

    # async def a_write_text(
    #         self,
    #         data: str,
    #         *,
    #         overwrite: bool = False,
    #         encoding: str = 'utf-8',
    #         errors: str = 'strict',
    # ) -> int:
    #     n = len(data)
    #     z = data.encode(encoding=encoding, errors=errors)
    #     await self.a_write_bytes(z, overwrite=overwrite)
    #     return n


def make_a_method(name):
    async def f(self, *args, **kwargs):
        f = partial(getattr(self, name), *args, **kwargs)
        return await asyncio.get_running_loop().run_in_executor(
            None, f
        )

    f.__name__ = f'a_{name}'
    return f


for m in ('copy_dir', 'copy_file',
          'export_dir', 'export_file',
          'exists',
          'file_info',
          'import_dir', 'import_file',
          'is_dir', 'is_file',
          'ls',
          'read_bytes', 'read_json', 'read_pickle', 'read_text',
          'remove_dir', 'remove_file',
          'rename_dir', 'rename_file',
          'rmrf',
          'write_bytes', 'write_json', 'write_pickle', 'write_text',
          ):
    setattr(Upath, f'a_{m}', make_a_method(m))
