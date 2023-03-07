from __future__ import annotations

import contextlib
import datetime
import os
import os.path
import pathlib
import shutil
import sys
import time
import warnings
from collections.abc import Iterator
from typing import Optional, Union

import filelock

# `filelock` is also called `py-filelock`.
# Tried `fasteners` also. In one use case,
# `filelock` worked whereas `fasteners.InterprocessLock` failed.
#
# Other options to look into include
# `oslo.concurrency`, `pylocker`, `portalocker`.
from deprecation import deprecated
from overrides import overrides

from ._upath import FileInfo, LockAcquireError, LockReleaseError, Upath

# End user may want to do this:
# logging.getLogger("filelock").setLevel(logging.WARNING)


# Hack `filelock` to replace `time.nomotonic` by `time.perf_counter`
def _acquire(
    self,
    timeout: float | None = None,
    poll_interval: float = 0.05,
    *,
    poll_intervall: float | None = None,
    blocking: bool = True,
) -> filelock.AcquireReturnProxy:
    """
    Try to acquire the file lock.
    :param timeout: maximum wait time for acquiring the lock, ``None`` means use the default :attr:`~timeout` is and
        if ``timeout < 0``, there is no timeout and this method will block until the lock could be acquired
    :param poll_interval: interval of trying to acquire the lock file
    :param poll_intervall: deprecated, kept for backwards compatibility, use ``poll_interval`` instead
    :param blocking: defaults to True. If False, function will return immediately if it cannot obtain a lock on the
        first attempt. Otherwise this method will block until the timeout expires or the lock is acquired.
    :raises Timeout: if fails to acquire lock within the timeout period
    :return: a context object that will unlock the file when the context is exited
    .. code-block:: python
        # You can use this method in the context manager (recommended)
        with lock.acquire():
            pass
        # Or use an equivalent try-finally construct:
        lock.acquire()
        try:
            pass
        finally:
            lock.release()
    .. versionchanged:: 2.0.0
        This method returns now a *proxy* object instead of *self*,
        so that it can be used in a with statement without side effects.
    """
    # Use the default timeout, if no timeout is provided.
    if timeout is None:
        timeout = self.timeout

    if poll_intervall is not None:
        msg = "use poll_interval instead of poll_intervall"
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        poll_interval = poll_intervall

    # Increment the number right at the beginning. We can still undo it, if something fails.
    with self._thread_lock:
        self._lock_counter += 1

    lock_id = id(self)
    lock_filename = self._lock_file
    start_time = time.perf_counter()
    try:
        while True:
            with self._thread_lock:
                if not self.is_locked:
                    filelock._api._LOGGER.debug(
                        "Attempting to acquire lock %s on %s", lock_id, lock_filename
                    )
                    self._acquire()

            if self.is_locked:
                filelock._api._LOGGER.debug(
                    "Lock %s acquired on %s", lock_id, lock_filename
                )
                break
            elif blocking is False:
                filelock._api._LOGGER.debug(
                    "Failed to immediately acquire lock %s on %s",
                    lock_id,
                    lock_filename,
                )
                raise filelock.Timeout(self._lock_file)
            elif 0 <= timeout < time.perf_counter() - start_time:
                filelock._api._LOGGER.debug(
                    "Timeout on acquiring lock %s on %s", lock_id, lock_filename
                )
                raise filelock.Timeout(self._lock_file)
            else:
                msg = "Lock %s not acquired on %s, waiting %s seconds ..."
                filelock._api._LOGGER.debug(msg, lock_id, lock_filename, poll_interval)
                time.sleep(poll_interval)
    except BaseException:  # Something did go wrong, so decrement the counter.
        with self._thread_lock:
            self._lock_counter = max(0, self._lock_counter - 1)
        raise
    return filelock.AcquireReturnProxy(lock=self)


# `filelock` is pinned to 3.9.0 with this hack.
# Once a new `filelock` version is out, check whether
# it has made similar changes.
filelock.BaseFileLock.acquire = _acquire


class LocalUpath(Upath, os.PathLike):
    _LOCK_POLL_INTERVAL_SECONDS = 0.03

    def __init__(self, *pathsegments: str):
        """
        Create a path on the local file system.
        Both POSIX and Windows platforms are supported.

        ``*pathsegments`` specify the path, either absolute or relative to the current
        working directory. If missing, the constructed path is the current working directory.
        This is passed to `pathlib.Path <https://docs.python.org/3/library/pathlib.html#pathlib.Path>`_.
        """
        super().__init__(str(pathlib.Path(*pathsegments).absolute()))
        self._lock_count: int = 0
        self._lock = None

    def __fspath__(self) -> str:
        """
        LocalUpath implements the `os.PathLike <https://docs.python.org/3/library/os.html#os.PathLike>`_ protocol,
        hence a LocalUpath object can be used anywhere an object implementing
        os.PathLike is accepted. For example, used with the builtin function
        `open() <https://docs.python.org/3/library/functions.html#open>`_:

        >>> p = LocalUpath('/tmp/test/data.txt')
        >>> p.rmrf()
        0
        >>> p.write_text('abc')
        >>> with open(p) as file:
        ...     print(file.read())
        abc
        >>> p.rmrf()
        1
        """
        return self.path.__fspath__()

    def __getstate__(self):
        return None, super().__getstate__()

    def __setstate__(self, data):
        _, z1 = data
        self._lock_count = 0
        self._lock = None
        super().__setstate__(z1)

    @property
    @overrides
    def path(self) -> pathlib.Path:
        """
        Return the `pathlib.Path <https://docs.python.org/3/library/pathlib.html#pathlib.Path>`_ object
        of the path.
        """
        return pathlib.Path(self._path)

    @property
    @deprecated(
        deprecated_in="0.6.9", removed_in="0.8.0", details="Use `path` instead."
    )
    def localpath(self):
        return self.path

    @overrides
    def as_uri(self) -> str:
        """
        Represent the path as a file URI.
        On Linux, this is like 'file:///home/username/path/to/file'.
        On Windows, this is like 'file:///C:/Users/username/path/to/file'.
        """
        return self.path.as_uri()

    @overrides
    def is_dir(self) -> bool:
        """
        Return whether the current path is a dir.
        """
        return self.path.is_dir()

    @overrides
    def is_file(self) -> bool:
        """
        Return whether the current path is a file.
        """
        return self.path.is_file()

    @overrides
    def file_info(self) -> Optional[FileInfo]:
        """
        Return file info if the current path is a file;
        otherwise return ``None``.
        """
        if not self.is_file():
            return None
        st = self.path.stat()
        return FileInfo(
            ctime=st.st_ctime,
            mtime=st.st_mtime,
            time_created=datetime.datetime.fromtimestamp(st.st_ctime),
            time_modified=datetime.datetime.fromtimestamp(st.st_mtime),
            size=st.st_size,
            details=st,
        )
        # If an existing file is written to again using `write_...`,
        # then its `ctime` and `mtime` are both updated.
        # My experiments showed that `ctime` and `mtime` are equal.

    @property
    @overrides
    def root(self) -> LocalUpath:
        """
        Return a new path representing the root.

        On Windows, this is the root on the same drive, like ``LocalUpath('C:\')``.
        On Linux and Mac, this is ``LocalUpath('/')``.
        """
        return self.__class__(self.path.root)

    @overrides
    def read_bytes(self) -> bytes:
        """
        Read the content of the current file as bytes.
        """
        try:
            return self.path.read_bytes()
        except (IsADirectoryError, FileNotFoundError) as e:
            raise FileNotFoundError(self) from e

    @overrides
    def write_bytes(self, data: bytes, *, overwrite: bool = False) -> None:
        """
        Write the bytes ``data`` to the current file.
        """
        if self.is_file():
            if not overwrite:
                raise FileExistsError(self)
        self.parent.path.mkdir(exist_ok=True, parents=True)
        self.path.write_bytes(data)

        # If `self` is an existing directory, will raise `IsADirectoryError`.
        # If `self` is an existing file, will overwrite.

    @overrides
    def _copy_file(self, target: Upath, *, overwrite: bool = False):
        if isinstance(target, LocalUpath):
            if not overwrite and target.is_file():
                raise FileExistsError(target)
            os.makedirs(target.parent, exist_ok=True)
            # If `p` is a file and we try to `os.makedirs(p / 'subdir`)`,
            # on Linux it raises `NotADirectoryError`;
            # on Windows it raises `FileNotFoundError`.
            shutil.copyfile(self.path, target.path)
            # If target already exists, it will be overwritten.
        else:
            super()._copy_file(target, overwrite=overwrite)

    @overrides
    def remove_dir(self, **kwargs) -> int:
        """
        Remove the current dir along with all its contents recursively.
        """
        n = super().remove_dir(**kwargs)
        if self.path.is_dir():
            shutil.rmtree(self.path)
        return n

    @overrides
    def remove_file(self) -> None:
        """Remove the current file."""
        try:
            self.path.unlink()
        except PermissionError as e:  # this happens on Windows if `self` is a dir.
            if self.is_dir():
                raise IsADirectoryError(self) from e
            else:
                raise
        # On Linux, if `self` is a dir, `IsADirectoryError` will be raised.

    def rename_dir(
        self,
        target: str | LocalUpath,
        *,
        overwrite: bool = False,
        quiet: bool = False,
    ) -> LocalUpath:
        """Rename the current dir (i.e. ``self``) to ``target``.

        ``overwrite`` is applied file-wise. If there are
        files under ``target`` that do not have counterparts under ``self``,
        they are left untouched.

        ``quiet`` controls whether to print progress info.

        Return the new path.
        """

        if not self.is_dir():
            raise FileNotFoundError(str(self))

        if isinstance(target, LocalUpath):
            target = target._path
        target_ = self.parent / target
        if target_ == self:
            return self

        if not quiet:
            print(f"Renaming {self!r} to {target_!r}", file=sys.stderr)
        self._copy_dir(self, target_, "rename_file", overwrite=overwrite, quiet=quiet)

        def _remove_empty_dir(path):
            k = 0
            for p in path.iterdir():
                if p.is_dir():
                    k += _remove_empty_dir(p)
                else:
                    k += 1
            if k == 0:
                path.rmdir()
            return k

        _remove_empty_dir(self.path)

        return target_

    def _rename_file(self, target: str, *, overwrite=False):
        target = self.parent / target
        if not overwrite and target.is_file():
            raise FileExistsError(target)
        os.makedirs(target.parent, exist_ok=True)
        self.path.rename(target.path)

    def rename_file(
        self, target: str | LocalUpath, *, overwrite: bool = False
    ) -> LocalUpath:
        """Rename the current file (i.e. ``self``) to ``target`` in the same store.

        ``target`` is either absolute or relative to ``self.parent``.
        For example, if ``self`` is '/a/b/c/d.txt', then
        ``target='e.txt'`` means '/a/b/c/e.txt'.

        If ``overwrite`` is ``False`` (the default) and the target file exists,
        ``FileExistsError`` is raised.

        Return the new path.
        """
        if isinstance(target, LocalUpath):
            target = target._path
        target_ = self.parent / target
        if target_ == self:
            return self

        self._rename_file(target_._path, overwrite=overwrite)
        return target_

    @overrides
    def iterdir(self) -> Iterator[LocalUpath]:
        """
        Yield the immediate children under the current dir.
        """
        try:
            for p in self.path.iterdir():
                yield self / p.name
        except (NotADirectoryError, FileNotFoundError):
            pass

    @overrides
    def riterdir(self) -> Iterator[LocalUpath]:
        """
        Yield all files under the current dir recursively.
        """
        for p in self.iterdir():
            if p.is_file():
                yield p
            elif p.is_dir():
                yield from p.riterdir()

    @contextlib.contextmanager
    @overrides
    def lock(self, *, timeout=None):
        """
        This uses the package `filelock <https://github.com/tox-dev/py-filelock>`_ to implement
        a file lock for inter-process communication.
        """
        if timeout is None:
            timeout = 60
        if self._lock_count == 0:
            os.makedirs(self.parent, exist_ok=True)
            lock = filelock.FileLock(str(self))
            t0 = time.perf_counter()
            try:
                lock.acquire(
                    timeout=timeout, poll_interval=self._LOCK_POLL_INTERVAL_SECONDS
                )
            except Exception as e:
                raise LockAcquireError(
                    f"waited on '{self}' for {time.perf_counter() - t0:.2f} seconds"
                ) from e
            self._lock = lock
        self._lock_count += 1
        try:
            yield
        finally:
            self._lock_count -= 1
            if self._lock_count == 0:
                try:
                    try:
                        self.remove_file()  # This must be done before releasing the lock, or lock can "leak".
                    except FileNotFoundError:
                        pass
                    self._lock.release(force=True)
                    self._lock = None
                except Exception as e:
                    raise LockReleaseError(f"failed to release lock file {self}") from e


LocalPathType = Union[str, pathlib.Path, LocalUpath]
