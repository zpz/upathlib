"""
``VersionedUploadable`` helps store and us a "dataset" in an exclusive directory in consistent and convenient ways. Specifically,

1. The dataset is identified by a version string that is generated and sortable (datetime-based),
   so that the "newest" is always the "latest" version, and code can infer the latest version.
   The full path of the storage location is managed for the user, who only needs the version.
2. The storage can be either local (on disk) or remote (in a cloud blobstore). There are methods to download/upload between local and remote storages.
3. Within the dataset, one can conveniently specify sub-directories and files relative to the "root",
   and read/write. This navigation is the same regardless of whether the storage is local or remote.
"""

from __future__ import annotations

import logging
import string
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from io import UnsupportedOperation
from typing import Any

from upathlib import BlobUpath, LocalUpath, Upath

logger = logging.getLogger(__name__)


ALNUM = string.ascii_letters + string.digits


def is_version(version: str) -> bool:
    # "[A-Za-z0-9][A-Za-z0-9._-]*"
    if not version:
        return False
    return (version[0] in ALNUM) and all(v in ALNUM or v in "._-" for v in version)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def make_version(tag: str = None) -> str:
    """
    Make a version string based on current UTC time in this format

    ::

        '20210816-082342-tag'

    where `'-tag'` is omitted if ``tag`` is falsy.

    Such version strings are sortable by time as there is practically no chance of collision
    between two versions.
    """
    ver = utcnow().strftime("%Y%m%d-%H%M%S")
    if tag:
        tag = tag.strip(" _-")
        if tag:
            assert is_version(tag)
            ver = ver + "-" + tag
    return ver


VERSION_STR_LEN = 8 + 1 + 6  # '20210816-082342'


class VersionExistsError(Exception):
    pass


class VersionNotFoundError(Exception):
    pass


class VersionedUploadable(ABC):
    """
    A subclass will customize :meth:`remote_cls_upath` and :meth:`local_cls_upath`.
    """

    @classmethod
    def resolve_version(
        cls, version: str, remote: bool | None = None
    ) -> tuple[str, bool]:
        if version == "latest-local":
            if remote is True:
                raise ValueError(
                    "version 'latest-local' is not valid with `remote=True`"
                )
            vers = cls.get_local_versions()
            if not vers:
                raise VersionNotFoundError(
                    f"could not find a local version of {cls.__name__}"
                )
            version = vers[-1]
            return version, False

        if version == "latest-remote":
            if remote is False:
                raise ValueError(
                    "version 'latest-remote' is not valid with `remote=False`"
                )
            vers = cls.get_remote_versions()
            if not vers:
                raise VersionNotFoundError(
                    f"could not find a remote version of {cls.__name__}"
                )
            version = vers[-1]
            return version, True

        if version == "latest":
            if remote is True:
                return cls.resolve_version("latest-remote", True)
            if remote is False:
                return cls.resolve_version("latest-local", False)
            assert remote is None
            v_remote = cls.get_remote_versions()
            v_local = cls.get_local_versions()
            if v_remote:
                v_r = v_remote[-1]
                if v_local:
                    v_l = v_local[-1]
                    if v_r > v_l:
                        return v_r, True
                    return v_l, False
                return v_r, True
            if v_local:
                v_l = v_local[-1]
                return v_l, False
            raise VersionNotFoundError(f"could not find a version of {cls.__name__}")

        assert is_version(version)
        # Exact version string (not 'latest...') are not checked for existence.
        return version, remote

    @classmethod
    def parse_version(cls, version: str) -> dict[str, str]:
        return {
            "datetime": version[:VERSION_STR_LEN],
            "tag": version[(VERSION_STR_LEN + 1) :],
        }

    @classmethod
    @abstractmethod
    def local_cls_upath(cls) -> LocalUpath:
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def remote_cls_upath(cls) -> BlobUpath:
        raise NotImplementedError

    @classmethod
    def get_local_versions(cls) -> list[str]:
        ll = (cls.local_cls_upath() / "versions").iterdir()
        return sorted(p.name for p in ll)

    @classmethod
    def get_remote_versions(cls) -> list[str]:
        ll = (cls.remote_cls_upath() / "versions").iterdir()
        return sorted(p.name for p in ll)

    @classmethod
    def has_local_version(cls, version: str) -> bool:
        return (cls.local_cls_upath() / "versions" / version / "info.json").is_file()

    @classmethod
    def has_remote_version(cls, version: str) -> bool:
        return (cls.remote_cls_upath() / "versions" / version / "info.json").is_file()

    @classmethod
    def rm_local_version(cls, version: str, **kwargs) -> None:
        (cls.local_cls_upath() / "versions" / version).remove_dir(**kwargs)

    @classmethod
    def rm_remote_version(cls, version: str, **kwargs) -> None:
        (cls.remote_cls_upath() / "versions" / version).remove_dir(**kwargs)

    @classmethod
    def new(
        cls, *, tag: str = None, remote: bool = False, **kwargs
    ) -> VersionedUploadable:
        remote = bool(remote)
        # Ensure this is not `None`.

        version = make_version(tag)

        if remote:
            upath = cls.remote_cls_upath() / "versions" / version
            assert not upath.is_file()
            assert not upath.is_dir()
        else:
            upath = cls.local_cls_upath() / "versions" / version
            assert not upath.is_file()
            if upath.is_dir():
                assert not list(upath.iterdir())  # empty directory
                upath.rmrf()

        obj = cls(version, remote=remote, require_exists=False, **kwargs)  # type: ignore
        # `kwargs`, if not empty, contains parameters accepted by `__init__` of a subclass
        return obj

    def __init__(
        self, version: str, *, remote: bool | None = None, require_exists: bool = True
    ):
        a, b = self.resolve_version(version, remote)
        #: version of the object
        self.version: str = a
        #: remote-ness of the object
        self.remote: bool = b

        if self.remote is None:
            # This is encountered only when `__init__` is called by user directly
            # (that is, not called by `new`), user specifies an exact `version` (that is,
            # not "latest", "latest-local", "latest-remote"), and `remote` is `None`).
            if self.has_local_version(self.version):
                # Prefer local version to remote version if the former exists.
                self.remote = False
            elif self.has_remote_version(self.version):
                self.remote = True
            else:
                raise VersionNotFoundError(
                    f"could not find version '{self.version}' of {self.__class__.__name__}"
                )

        try:
            self.info = self.path("info.json").read_json()
        except FileNotFoundError:
            if require_exists:
                raise VersionNotFoundError(
                    f"{'remote' if self.remote else 'local'} version '{self.version}' of {self.__class__.__name__} does not exist"
                )
            self.info = {}

        # A subclass usually should call `super().__init__` to settle `self.version` and `self.remote`,
        # then init object attributes to default values or load them if certain files exist.
        #
        # A subclass often needs to check `self.exists()` before loading things.
        # It may also do different things according to the value of `self.remote`.

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(version='{self.version}', remote={self.remote})"
        )

    def __str__(self) -> str:
        return self.__repr__()

    def as_local(self, *, require_exists: bool = True, **kwargs) -> VersionedUploadable:
        """
        Return the *local* object of this version of this class.
        If ``self`` is local, then return ``self``.
        Otherwise, return a new object.

        This method does not modify ``self``.

        `kwargs`: additional parameters defined and required by a subclass.
        """
        if not self.remote:
            return self
        return self.__class__(
            self.version,
            remote=False,
            require_exists=require_exists,
            **kwargs,
        )

    def as_remote(
        self, *, require_exists: bool = True, **kwargs
    ) -> VersionedUploadable:
        if self.remote:
            return self
        return self.__class__(
            self.version,
            remote=True,
            require_exists=require_exists,
            **kwargs,
        )

    @property
    def local_upath(self) -> LocalUpath:
        """
        Root directory of this version on the local storage,
        regardless of whether ``self`` is local or remote.
        """
        return self.local_cls_upath() / "versions" / self.version

    @property
    def remote_upath(self) -> BlobUpath:
        """
        Root directory of this version on the remote storage,
        regardless of whether ``self`` is local or remote.
        """
        return self.remote_cls_upath() / "versions" / self.version

    @property
    def upath(self) -> Upath:
        """
        Return the root directory of ``self``.

        This is consistent with the remote-ness of ``self``.
        """
        if self.remote:
            return self.remote_upath
        return self.local_upath

    def path(self, *args: str) -> Upath:
        """
        Return a path relative to the root directory of ``self``.

        Examples:

            self.path()                               # the root path
            self.path('info.json')                    # file in root directory
            self.path('abc', 'de', 'data.parquet')    # file 'abc/de/data.parquet' under root directory
            self.path('abc/de/data.parquet')          # same as above

        Typically, you proceed to read or write with the returned path (object), e.g.,

        ::

            self.path('info.json').write_json(self.info, overwrite=True)
            info = self.path('info.json').read_json()

        This is consistent with the remote-ness of ``self``.
        In other words, if ``self`` is local, then the returned path is local (under the local root directory);
        otherwise, the returned path is remote.

        .. note:: Don't start ``args`` with ``'/'``.
        """
        return self.upath.joinpath(*args)

    def exists(self) -> bool:
        """
        A version is considered existent if and only if the file "info.json"
        exists in its root directory.
        """
        return self.path("info.json").is_file()

    def save(self) -> None:
        """
        A subclass should re-implement this method to save its own stuff
        like data, summary, and whatever, and in the end call
        ``super().save()``.
        """
        self.path("info.json").write_json(self.info, overwrite=True)

    def download(self, path: str = None, *, overwrite: bool = False, **kwargs) -> int:
        """
        Download the entire dataset or specified parts of it.

        If the current object already points to a local version, then
        ``UnsupportedOperation`` is raised.

        If you know certain files have changed, you can bring remote/local into sync
        by downloading/uploading those particular files.

        Parameters
        ----------
        path
            Specific subdirectory or file to download.
            If ``None``, the entire version is downloaded.

            If ``None``, and the local version already exists, and ``overwrite`` is ``False``,
            then download will not happen. However, if the local version is incomplete or corrupt compared
            to the remote counterpart (the same version), the code wouldn't know.

            If not ``None``, then the specified sub-directory or file will be downloaded
            (into the expected locations for the version).
            This is meant for "repair work" if you know certain parts of the local version are corrupt or missing.
            If the version does not exist locally, there is hardly a scenario for downloading only parts of it
            (and that may cause issues later, as you are creating an incomplete local version).

        overwrite
            If ``True``, overwrite any file that exists locally.

        **kwargs
            If ``path`` is ``None``, this is passed on to ``upathlib.Upath.download_dir``.
            If ``path`` is not ``None``, this is ignored.

        Returns
        -------
        int
            The number of files downloaded.

        Warnings
        --------
        You should not use ``overwrite=True`` lightly just to ensure it proceeds.
        The default ``overwrite=False`` prevents re-downloading when the local version
        already exists. Try to benefit from such savings as far as you can.
        """
        if not self.remote:
            raise UnsupportedOperation(
                "can not download an object that is in local mode"
            )
        if not self.exists():
            raise RuntimeError(f"object {self!r} does not exist")

        source = self.upath
        target = self.local_upath

        if path:
            source = source / path
            target = target / path
            if source.is_file():
                return source.download_file(target, overwrite=overwrite)
            else:
                return source.download_dir(target, overwrite=overwrite)

        if not overwrite:
            if self.as_local(require_exists=False).exists():
                logger.info("local version of %r exists; upload is skipped", self)
                return 0
        return source.upload_dir(target, overwrite=True, **kwargs)

    def upload(self, path: str = None, *, overwrite: bool = False, **kwargs) -> int:
        """
        Analogous to ``download``.

        Return the number of files uploaded.
        """
        if self.remote:
            raise UnsupportedOperation(
                "can not upload an object that is in remote mode"
            )
        if not self.exists():
            raise RuntimeError(f"object {self!r} does not exist")

        source = self.upath
        target = self.remote_upath

        if path:
            source = source / path
            target = target / path
            if source.is_file():
                return target.upload_file(source, overwrite=overwrite)
            else:
                return target.upload_dir(source, overwrite=overwrite)
        if not overwrite:
            if self.as_remote(require_exists=False).exists():
                logger.info("remote version of %r exists; upload is skipped", self)
                return 0
        return target.upload_dir(source, overwrite=True, **kwargs)

    def ensure_local(
        self, *, init_kwargs: dict[str, Any] = None, **kwargs
    ) -> VersionedUploadable:
        """
        Return a local object of this version that exists.

        If ``self`` is local, then ``self`` is returned.

        Otherwise, if the local version does not exist, it will be downloaded.
        If the local version exists, downloading will not happen.
        (This code has to assume the local version is sound. though.)
        To force downloading regardless, pass in ``overwrite=True``, but don't do that lightly!

        Parameters
        ----------
        init_kwargs
            For special needs of a subclass that defines additional arguments for its ``__init__``.

        **kwargs
            Passed on to ``download``.

        .. note:: Calling ``ensure_local`` does not make the current object local;
            you need to receive and use the returned object, which *is* local.
        """
        if not self.remote:
            return self
        self.download(**kwargs)
        return self.as_local(**(init_kwargs or {}))
