from __future__ import annotations

# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import abc
import contextlib
import datetime
import os
import os.path
import pathlib
import queue
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from io import UnsupportedOperation
from typing import (
    List,
    Iterable,
    Iterator,
    Type,
    TypeVar,
    Any,
    Optional,
    Tuple,
    Callable,
)

from overrides import EnforceOverrides
from tqdm.auto import tqdm
from .serializer import (
    ByteSerializer,
    TextSerializer,
    JsonSerializer,
    ZJsonSerializer,
    ZstdJsonSerializer,
    PickleSerializer,
    ZPickleSerializer,
    ZstdPickleSerializer,
    OrjsonSerializer,
    ZOrjsonSerializer,
    ZstdOrjsonSerializer,
)

# End user may want to do this:
#  logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
# to suppress the "urllib3 connection lost" warning.

T = TypeVar("T", bound="Upath")


class LockAcquireError(TimeoutError):
    pass


class LockReleaseError(RuntimeError):
    pass


@dataclass
class FileInfo:
    ctime: float  # creation POSIX timetamp
    mtime: float  # last modification POSIX timestamp
    time_created: datetime.datetime
    time_modified: datetime.datetime
    size: int  # in bytes
    details: Any  # platform-dependent


class Upath(abc.ABC, EnforceOverrides):  # pylint: disable=too-many-public-methods
    @classmethod
    def register_read_write_byte_format(cls, serde: Type[ByteSerializer], name: str):
        """
        For example, if `serde` is a ByteSerializer subclass and `name` is 'myway',
        then this method adds isinstance methods `write_myway` and `read_myway`.

        `name`: usually is a slight variation of the name of the class `serde`,
            with changes such as lower-casing and separating words by underscores.
            Needs to be a valid method name, e.g. can't contain space or dash.
        """

        def _write(self, data, *, overwrite=False, **kwargs):
            return self.write_bytes(
                serde.serialize(data, **kwargs), overwrite=overwrite
            )

        def _read(self, **kwargs):
            z = self.read_bytes()
            return serde.deserialize(z, **kwargs)

        setattr(_write, "__name__", f"write_{name}")
        setattr(_read, "__name__", f"read_{name}")
        setattr(cls, f"write_{name}", _write)
        setattr(cls, f"read_{name}", _read)

    @classmethod
    def register_read_write_text_format(cls, serde: Type[TextSerializer], name: str):
        """
        Anologous to `register_read_write_byte_format`.
        """

        def _write(self, data, *, overwrite=False, **kwargs):
            return self.write_text(serde.serialize(data, **kwargs), overwrite=overwrite)

        def _read(self, **kwargs):
            z = self.read_text()
            return serde.deserialize(z, **kwargs)

        setattr(_write, "__name__", f"write_{name}")
        setattr(_read, "__name__", f"read_{name}")
        setattr(cls, f"write_{name}", _write)
        setattr(cls, f"read_{name}", _read)

    def __init__(
        self, *pathsegments: str, thread_pool_executors: List[ThreadPoolExecutor] = None
    ):
        """`Upath` is the base class for a client to a blob store,
        including local file system as a special case.

        `*pathsegments`: analogous to the input to `pathleib.Path`.
        The first segment may or may not start with `/`; it makes
        no difference. The path constructed with `*pathsegments`
        is always "absolute" under a known "root".

            Note that if one segment starts with '/', it will reset to the "root"
            and discard all the segments that have come before it.

            If missing, the path constructed is the "root".

        For a local POSIX file system, the root is the usual `/`.
        For Azure blob store, the root is that in a "container".
        For AWS and GCP blob stores, the root is that in a "bucket".

        `thread_pool_executors`: some operations may use threads. If there are
        a large number of Upath instances active at the same time, the number of
        threads could be large. You may pass in two thread-pool-executors to use,
        hence controlling the total number of threads created by this object.
        This parameter consists of two *separate* executors---don't pass in
        a single executor twice.

        Subclasses for cloud blob stores may need to add additional parameters
        representing, e.g., container/bucket name, etc.
        """

        self._path = os.path.normpath(
            os.path.join("/", *pathsegments)
        )  # pylint: disable=no-value-for-parameter
        # The path is always "absolute" starting with '/'.
        # It does not have a trailing `/` unless the path is just `/` itself.

        if thread_pool_executors is None:
            self._thread_pools = []
        elif thread_pool_executors == []:
            self._thread_pools = thread_pool_executors
        else:
            assert len(thread_pool_executors) == 2
            e0, e1 = thread_pool_executors
            e0._thread_name_prefix = "UpathExecutor0"
            e1._thread_name_prefix = "UpathExecutor1"
            self._thread_pools = thread_pool_executors

    def __getstate__(self):
        return (self._path,)

    def __setstate__(self, data):
        self._path = data[0]
        self._thread_pools = []

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self._path}')"
        # Subclass may want to customize this method to add more info,
        # e.g. "bucket" name.

    def __str__(self) -> str:
        return self._path

    def __eq__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        return self._path == other._path
        # Subclass may want to customize this method to check more things,
        # e.g. whether `self` and `other` are in the same "bucket".

    def __lt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        return self._path < other._path

    def __le__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        return self._path <= other._path

    def __gt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        return self._path > other._path

    def __ge__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        return self._path >= other._path

    def __hash__(self) -> int:
        return hash(repr(self))

    def __truediv__(self: T, key: str) -> T:
        """
        This is called by `self / key`.
        """
        return self.joinpath(key)

    @property
    def _thread_pool_executors(self):
        if not self._thread_pools:
            self._thread_pools.append(
                ThreadPoolExecutor(thread_name_prefix="UpathExecutor0")
            )
            self._thread_pools.append(
                ThreadPoolExecutor(10, thread_name_prefix="UpathExecutor1")
            )
        return self._thread_pools
        # Currently there can be two "layers" of threads running during `download_dir`.
        # In `download_dir`, the download of each file runs in the threads provided
        # by `nest0`. If a file is large, `GcsBlobUpath` will split the work into chunks
        # and download each chunk in a thread provided by `nest1`.
        # We dedicate the two executors mainly so that the second layer of chunk downloads
        # do not starve for threads.

    def _run_in_executor(
        self,
        tasks: Iterable[Tuple[Callable, tuple, dict, str]],
        description: str,
    ):
        """
        This method is used to run multiple I/O jobs concurrently, e.g.
        uploading/downloading all files in a folder recursively.

        `tasks`: each element is a tuple of (func, args, kwargs, description).

        `description`: description of the entire set of tasks, such as "Downloading directory 'abc'".
            Note: if `description` is a false value, progress printouts will be suppressed.
        """
        if not isinstance(tasks, list):
            tasks = list(tasks)
        n_tasks = len(tasks)
        if not n_tasks:
            return

        def enqueue(q_tasks, executor, q):
            while True:
                try:
                    func, args, kwargs, desc = q_tasks.get_nowait()
                except queue.Empty:
                    break
                t = executor.submit(func, *args, **kwargs)
                q.put((t, desc))
            q.put(None)

        pbar = None
        if threading.current_thread().name.startswith("UpathExecutor0"):
            # This is the case when GCP downloads a large file by multiple parts.
            executor = self._thread_pool_executors[1]
        else:
            executor = self._thread_pool_executors[0]
            if description:
                print(description, file=sys.stderr)
                pbar = tqdm(
                    total=n_tasks,
                    bar_format="{percentage:5.1f}%, {n:.0f}/{total_fmt}, {elapsed} | {desc}",
                )

        try:
            q = queue.Queue(executor._max_workers + 4)
            q_tasks = queue.SimpleQueue()
            for t in tasks:
                q_tasks.put(t)
            task = threading.Thread(target=enqueue, args=(q_tasks, executor, q))
            task.start()

            try:
                while True:
                    z = q.get()
                    if z is None:
                        break
                    t, desc = z
                    if pbar:
                        pbar.set_description_str(desc)
                        pbar.update(0.5)
                    try:
                        yield t.result()
                    except Exception:
                        while True:
                            try:
                                _ = q_tasks.get_nowait()
                            except queue.Empty:
                                break
                        while True:
                            z = q.get()
                            if z is None:
                                break
                            t, desc = z
                            t.cancel()
                            # This may not succeed, but there isn't a good way to
                            # guarantee cancellation here.
                        raise
                    if pbar:
                        pbar.update(0.5)
            finally:
                task.join()
        finally:
            if pbar:
                pbar.close()

    def copy_dir(
        self,
        target: str,
        *,
        overwrite: bool = False,
        quiet: bool = False,
    ) -> int:
        """Analogous to `copy_file`.

        Return the number of files copied.
        """
        target_ = self.parent / target
        if target_ == self:
            return 0

        def foo():
            for p in self.riterdir():
                extra = str(p.path.relative_to(self.path))
                yield (
                    p.copy_file,
                    ((target_ / extra)._path,),
                    {"overwrite": overwrite},
                    extra,
                )

        if quiet:
            desc = False
        else:
            desc = f"Copying from {self} into {target_}"

        n = 0
        for _ in self._run_in_executor(foo(), desc):
            n += 1
        return n

    def _copy_file(self: T, target: T, *, overwrite: bool = False) -> None:
        # `target` is a path in the same store.
        # Reference implementation.
        # Subclass may customize this to perform file operations.
        target.write_bytes(self.read_bytes(), overwrite=overwrite)

    def copy_file(self, target: str, *, overwrite: bool = False) -> None:
        """Copy file to `target` in the same store.

        `target` is either absolute, or relative to `self.parent`.
        For example, if `self` is '/a/b/c/d.txt', then
        `target='e.txt'` means '/a/b/c/e.txt'.

        If `self` is not an existing file, raise `FileNotFoundError`.

        If `target` is an existing file and `overwrite` is `False`,
        raise `FileExistsError`.

        If `target` is an existing directory and `type(self)` is `LocalUpath`,
        raise `IsADirectoryError`. In a cloud blob store, this operation may be
        allowed, although the user is recommended to avoid such naming.
        """
        target_ = self.parent / target
        if target_ == self:
            return

        self._copy_file(target_, overwrite=overwrite)

    def exists(self) -> bool:
        """Return `True` if the path is an existing file or dir,
        `False` otherwise.

        In a blobstore with blobs

            /a/b/cd
            /a/b/cd/e.txt

        '/a/b/cd' exists, and is both a file and a dir;
        '/a/b/cd/e.txt' exists, and is a file;
        '/a/b' exists, and is a dir;
        '/a/b/c' does not exist.
        """
        return self.is_file() or self.is_dir()

    def export_dir(
        self,
        target: Upath,
        *,
        overwrite: bool = False,
        quiet: bool = False,
    ) -> int:
        """Copy the content of the current directory recursively
        to the specified `target`, which is typically in another store.

        `target` corresponds to `self`, that is, direct children of `self`
        are copied directly into `target`.

        Compare with `copy_dir`, which makes copies within the *same store*.

        Overwriting happens file-wise. For example, if the target directory
        contains files that do not exist in the source directory, they
        are left untouched.

        If `target` is a `LocalUpath` object and `self` is a `BlobUpath` object,
        then a subclass may implement more efficient ways to "download",
        along with renaming this method to "download_dir".

        Return the number of files copied.
        """

        def foo():
            self_path = self.path
            for p in self.riterdir():
                extra = str(p.path.relative_to(self_path))
                yield (
                    p.export_file,
                    (target / extra,),
                    {"overwrite": overwrite},
                    extra,
                )

        if quiet:
            desc = False
        else:
            desc = f"Exporting from {self!r} into {target!r}"

        n = 0
        for _ in self._run_in_executor(foo(), desc):
            n += 1
        return n

    def export_file(self, target: Upath, *, overwrite: bool = False) -> None:
        """Copy the file to the specified `target`, which is typically
        in another store.

        The `target` specifies a blob corresponding to `self`.

        If `target` is a `LocalUpath` object representing an existing *directory*,
        `IsADirectoryError` is raised. A copy is not placed *into* the target directory.
        This behavior differs from the Linux command `cp`.

        If `target` is a path in a cloud store, and is an existing *directory*,
        a new blob may be created as a result of this "export", because in cloud stores
        a path can be both a "file" and a "directory", although user is recommended
        to avoid such namings.

        Compare with `copy_file`, which makes copies within the same store.
        """
        # Reference implementation.
        # Subclass may customize this to perform file download
        # when `target` is a `LocalUpath`.
        target.write_bytes(self.read_bytes(), overwrite=overwrite)

    @abc.abstractmethod
    def file_info(self) -> Optional[FileInfo]:
        """
        If `self.is_file()` is `False`, return `None`.
        """
        raise NotImplementedError

    def import_dir(
        self,
        source: Upath,
        *,
        overwrite: bool = False,
        quiet: bool = False,
    ) -> int:
        """Analogous to `export_dir`."""

        def foo():
            source_path = source.path
            for p in source.riterdir():
                extra = str(p.path.relative_to(source_path))
                yield (
                    (self / extra).import_file,
                    (p,),
                    {"overwrite": overwrite},
                    extra,
                )

        if quiet:
            desc = False
        else:
            desc = f"Importing from {source!r} into {self!r}"

        n = 0
        for _ in self._run_in_executor(foo(), desc):
            n += 1
        return n

    def import_file(self, source: Upath, *, overwrite: bool = False) -> None:
        """
        If `self` is a `LocalUpath` object representing an existing *directory*,
        `IsADirectoryError` is raised.

        When `source` is a `LocalUpath` and `self` is a `BlobUpath` object,
        subclass may implement this in more efficient ways for uploading,
        along with renaming it to `upload_file`.
        """
        self.write_bytes(source.read_bytes(), overwrite=overwrite)

    @abc.abstractmethod
    def is_dir(self) -> bool:
        """Return `True` if the path is an existing directory, `False` otherwise.

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
        there exist files named `/a/b/*`.

        In a local file system, there can be empty directories.
        However, it is recommended to not have empty directories.

        There is no method for "creating a dir" (like `mkdir`).
        Simply create a file under the dir, and the dir will come into being.
        This is analogous to our treatment to files---we don't "create" a file
        in advance; we simply write to a path, intending it to be a file.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def is_file(self) -> bool:
        """Return `True` if the path is an existing file, `False` otherwise.

        In a cloud blob store, a path can be both a file and a dir. For
        example, if these blobs exist:

            /a/b/c/d.txt
            /a/b/c

        we say `/a/b/c` is a "file", and also a "dir".
        User is recommended to avoid such namings.

        This situation does not happen in a local file system.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def iterdir(self: T) -> Iterator[T]:
        """Yield the first-level (i.e. non-recursive) children
        of the current dir.

        Each yielded element is either a file or a dir.

        If `self` is not a dir (e.g. maybe it's a file),
        or does not exist at all, yield nothing (resulting in an
        empty iterable), but do not raise exception.

        There is no guarantee on the order of the returned elements.
        """
        raise NotImplementedError

    def joinpath(self: T, *other: str) -> T:
        """Join this path with more segments, return the new path object."""
        return self.with_path(self._path, *other)

    @contextlib.contextmanager
    @abc.abstractmethod
    def lock(self, *, timeout: int = None):
        """Lock the file pointed to, in order to have exclusive access.

        `timeout`: if the lock can't be acquired within *timeout* seconds,
        raise `LockAcquireError`. If `None`, wait for a default
        reasonably long time. To wait "forever", just pass in a large number.
        Once a lease is acquired, it will not expire until this contexmanager exits.
        In other words, this is timeout for the "wait", not for the
        lease itself. Actual waiting time may be slightly longer.

        This is a "mandatory lock", as opposed to an "advisory lock".
        However, this API does not specify that the locked file
        can be accessed for its content or used in any particular way.
        The intended use case is for this lock to be used
        for implementing a (cooperative) "code lock".

        The `yield` statement is not required to yield anything,
        that is, it may be simply

            yield

        rather than, say,

            yield self

        One way to achive cooperative locking on a file via this mandatory
        lock is like this:

            f = Upath('abc.txt')
            with f.with_suffix('.txt.lock').lock():
                ...
                # now write `f` with exclusive access,
                # because any other (cooperative) code block
                # will not be able to get hold of `abc.txt.lock`
                # in order to write `f` in its context-managed block.
                # It's up to the program design whether this lock
                # covers reading as well.

        Some storage engines may not provide the capability to implement
        this lock.
        """
        raise NotImplementedError

    def ls(self: T) -> List[T]:
        return sorted(self.iterdir())

    @property
    def name(self) -> str:
        """Return the segment after the last `/`.

        If `self.path` is '/', then `self.path.name` is ''.
        """
        return self.path.name

    @property
    def parent(self: T) -> T:
        return self.with_path(str(self.path.parent))

    @property
    def path(self) -> pathlib.PurePosixPath:
        return pathlib.PurePosixPath(self._path)

    @abc.abstractmethod
    def read_bytes(self) -> bytes:
        """Return the binary contents of the file.

        If `self` is not a file or is non-existent,
        raise `FileNotFoundError`.
        """
        raise NotImplementedError

    def read_text(self):
        # Refer to https://docs.python.org/3/library/functions.html#open
        return self.read_bytes().decode(encoding="utf-8", errors="strict")

    def remove_dir(self, *, quiet: bool = True) -> int:
        """Remove the directory pointed to by `self`,
        along with all its contents, recursively.

        Return the number of files removed.

        Local upath needs to customize this implementation, because
        it needs to take care of deleting "empty" subdirectories.
        """

        def foo():
            for p in self.riterdir():
                yield p.remove_file, [], {}, str(p.path.relative_to(self.path))

        if quiet:
            desc = False
        else:
            desc = f"Removing {self!r}"

        n = 0
        for _ in self._run_in_executor(foo(), desc):
            n += 1
        return n

    @abc.abstractmethod
    def remove_file(self) -> None:
        """Remove the file pointed to by `self`.

        If `self` is not an existing file, raise FileNotFoundError.
        If the file exists but can't be removed, usually the platform-dependent
        exception is propagated.
        """
        raise NotImplementedError

    def rename_dir(
        self: T,
        target: str,
        *,
        overwrite: bool = False,
        quiet: bool = False,
    ) -> T:
        """Analogous to `rename_file`.

        `overwrite` is applied per file, which suggests that if there are
        files under `target` that do not have counterparts under `self`,
        they are left untouched.

        Local upath needs to customize this implementation, because
        it needs to take care to delete empty subdirectories under `self`.
        """
        if not self.is_dir():
            raise FileNotFoundError(str(self))

        target_ = self.parent / target
        if target_ == self:
            return self

        def foo():
            for p in self.riterdir():
                extra = str(p.path.relative_to(self.path))
                yield (
                    p.rename_file,
                    [(target_ / extra)._path],
                    {"overwrite": overwrite},
                    extra,
                )

        if quiet:
            desc = False
        else:
            desc = f"Renaming {self!r} to {target_!r}"

        for _ in self._run_in_executor(foo(), desc):
            pass
        return target_

    def _rename_file(self: T, target: str, *, overwrite: bool = False):
        """Rename `self` to `target`, which is a path in the same store.

        This is a reference implementation. There are likely
        more efficient ways to do this on any specific platform.
        """
        self.copy_file(target, overwrite=overwrite)
        self.remove_file()

    def rename_file(self: T, target: str, *, overwrite: bool = False) -> T:
        """Rename the current file to `target` in the same store.

        `target` is either absolute or relative to `self.parent`.
        For example, if `self` is '/a/b/c/d.txt', then
        `target='e.txt'` means '/a/b/c/e.txt'.

        Return an object pointing to the new path.
        """
        target_ = self.parent / target
        if target_ == self:
            return self

        self._rename_file(target_._path, overwrite=overwrite)
        return target_

    @abc.abstractmethod
    def riterdir(self: T) -> Iterator[T]:
        """Yield files under the current dir recursively.

        Compared to `iterdir`, this is recursive, and yields
        *files* only. Empty subdirectories will have no representation
        in the return.

        Similar to `iterdir`, if `self` is not a dir or does not exist,
        then nothing is yielded, and no exception is raised either.

        There is no guarantee on the order of the returned elements.
        """
        raise NotImplementedError

    def rmrf(self, *, quiet: bool = True) -> int:
        """Analogous to `rm -rf`. Remove the file or dir pointed to
        by `self`.

        Return the number of files removed.

        For example, if these blobs are present:

            /a/b/c/d/e.txt
            /a/b/c/kk.data
            /a/b/c

        then `Upath('/a/b/c').rmrf()` would remove all of them.
        """
        if self._path == "/":
            raise UnsupportedOperation("`rmrf` not allowed on root directory")
        try:
            self.remove_file()
        except (FileNotFoundError, IsADirectoryError):
            n = 0
        else:
            n = 1
        try:
            m = self.remove_dir(quiet=quiet)
        except FileNotFoundError:
            m = 0
        return n + m

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
        return self.with_path(str(self.path.with_name(name)))

    def with_path(self: T, *paths) -> T:
        """
        Returns a new object of the same class at the specified path.
        The new path is unrelated to the current path; in other words,
        the new path is not "relative" to the current path.

        Meta data such as account info remains the same.

        Subclass needs to reimplement this method
        if its `__init__` expects additional args.
        """
        return self.__class__(*paths, thread_pool_executors=self._thread_pools)

    # def with_stem(self: T, stem: str) -> T:
    #     # Available in Python 3.9+.
    #     return self.with_path(str(self.path.with_stem(stem)))

    def with_suffix(self: T, suffix: str) -> T:
        """`suffix` should include a dot, like '.txt'.
        If `suffix` is '', the effect is to remove the existing suffix.
        """
        return self.with_path(str(self.path.with_suffix(suffix)))

    @abc.abstractmethod
    def write_bytes(self, data: bytes, *, overwrite: bool = False) -> None:
        """Write bytes to file.

        Parent directories are created as needed.

        `overwrite`: overwrite existing file?
            If `False`, and file exists, raises `FileExistsError`.
        """
        raise NotImplementedError

    def write_text(
        self,
        data: str,
        *,
        overwrite: bool = False,
    ) -> None:
        z = data.encode(encoding="utf-8", errors="strict")
        self.write_bytes(z, overwrite=overwrite)


# Add methods
# 'read_json', 'write_json', 'read_json_z', 'write_json_z', 'read_json_zstd', 'write_json_zstd',
# 'read_pickle', 'write_pickle', 'read_pickle_z', 'write_pickle_z', 'read_pickle_zstd', 'write_pickle_zstd',
# 'read_orjson', 'write_orjson', 'read_orjson_z', 'write_orjson_z', 'read_orjson_zstd', 'write_orjson_zstd',
#
# Applications can follow these examples to define and register their custom formats.

Upath.register_read_write_text_format(JsonSerializer, "json")
Upath.register_read_write_byte_format(ZJsonSerializer, "json_z")
Upath.register_read_write_byte_format(ZstdJsonSerializer, "json_zstd")
Upath.register_read_write_byte_format(PickleSerializer, "pickle")
Upath.register_read_write_byte_format(ZPickleSerializer, "pickle_z")
Upath.register_read_write_byte_format(ZstdPickleSerializer, "pickle_zstd")
Upath.register_read_write_byte_format(OrjsonSerializer, "orjson")
Upath.register_read_write_byte_format(ZOrjsonSerializer, "orjson_z")
Upath.register_read_write_byte_format(ZstdOrjsonSerializer, "orjson_zstd")
