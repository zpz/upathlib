from __future__ import annotations

# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import contextlib
import logging
import os
import time
from collections.abc import Iterator
from datetime import datetime
from io import BufferedReader, UnsupportedOperation, BytesIO
from typing import Optional

import google.auth
import opnieuw
import requests
from google import resumable_media
from google.cloud import storage
from google.api_core.exceptions import (
    NotFound,
    PreconditionFailed,
    TooManyRequests,
    GatewayTimeout,
    ServiceUnavailable,
)
from overrides import overrides

from ._upath import FileInfo, LockAcquireError, LockReleaseError, Upath
from ._blob import BlobUpath, LocalPathType, _resolve_local_path


logger = logging.getLogger(__name__)


# 67108864 = 256 * 1024 * 256 = 64 MB
MEGABYTES32 = 33554432
MEGABYTES64 = 67108864
LARGE_FILE_SIZE = MEGABYTES64


RETRY_WRITE_ON_EXCEPTIONS = (
    TooManyRequests,
    GatewayTimeout,
    ServiceUnavailable,
    requests.ReadTimeout,
    requests.ConnectionError,
)


class GcsBlobUpath(BlobUpath):
    """
    GcsBlobUpath implements the :class:`~upathlib.Upath` API for
    Google Cloud Storage.
    """

    _PROJECT_ID: str = None
    _CREDENTIALS: google.auth.credentials.Credentials = None
    _CLIENT: storage.Client = None

    @classmethod
    def _client(cls) -> storage.Client:
        """
        Return a client to the GCS service.

        If you have GCP account_info in a dict with these elements
        (not sure everything here is required)::

            'type': 'service_account',
            'project_id':
            'private_key_id':
            'private_key':
                '-----BEGIN PRIVATE KEY-----\\n'
                + private_key.encode('latin1').decode('unicode_escape')
                + '\\n-----END PRIVATE KEY-----\\n',
            'client_email':
            'client_id':
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
            'client_x509_cert_url': f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}"

        then ``credentials`` are obtained by

        ::

            google.oauth2.service_account.Credentials.from_service_account_info(
                account_info, scopes=['https://www.googleapis.com/auth/cloud-platform'])

        Code that runs on a GCP machine may be able to infer ``credentials`` and ``project_id``
        via `google.auth.default() <https://googleapis.dev/python/google-auth/latest/user-guide.html#application-default-credentials>`_.
        """
        refresh = False
        if cls._CLIENT is None:
            refresh = True
            if cls._PROJECT_ID is None or cls._CREDENTIALS is None:
                cred, pid = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                if cls._CREDENTIALS is None:
                    cls._CREDENTIALS = cred
                if cls._PROJECT_ID is None:
                    cls._PROJECT_ID = pid
        if (
            not cls._CREDENTIALS.token
            or (cls._CREDENTIALS.expiry - datetime.utcnow()).total_seconds()
            < 300  # 5 minutes
        ):
            refresh = True
            cls._CREDENTIALS.refresh(google.auth.transport.requests.Request())
            # One check shows that this token expires in one hour.
        if refresh:
            cls._CLIENT = storage.Client(
                project=cls._PROJECT_ID, credentials=cls._CREDENTIALS
            )
        return cls._CLIENT

    def __init__(
        self,
        *paths: str,
        bucket_name: str = None,
    ):
        """
        If ``bucket_name`` is ``None``, then ``*paths`` should be a single string
        starting with 'gs://<bucket-name>/'.

        If ``bucket_name`` is specified, then ``*paths`` specify path components
        under the root of the bucket.

        Examples
        --------
        These several calls are equivalent:

        >>> GcsBlobUpath('experiments', 'data', 'first.data', bucket_name='backup')
        >>> GcsBlobUpath('/experiments/data/first.data', bucket_name='backup')
        >>> GcsBlobUpath('gs://backup/experiments/data/first.data')
        """
        if bucket_name is None:
            assert len(paths) == 1
            path = paths[0]
            assert path.startswith("gs://")
            path = path[5:]
            k = path.find("/")
            if k < 0:
                bucket_name = path
                paths = ("/",)
            else:
                bucket_name = path[:k]
                paths = (path[k:],)

        super().__init__(*paths)
        self.bucket_name = bucket_name
        self._bucket_ = None
        self._blob_ = None
        self._lock_count: int = 0
        self._generation = -1
        self._quiet_multidownload = True

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.as_uri()}')"

    def __str__(self) -> str:
        return self.as_uri()

    def __eq__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other.bucket_name != self.bucket_name:
            return NotImplemented
        return self._path == other._path

    def __lt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other.bucket_name != self.bucket_name:
            return NotImplemented
        return self._path < other._path

    def __le__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other.bucket_name != self.bucket_name:
            return NotImplemented
        return self._path <= other._path

    def __gt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other.bucket_name != self.bucket_name:
            return NotImplemented
        return self._path > other._path

    def __ge__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other.bucket_name != self.bucket_name:
            return NotImplemented
        return self._path >= other._path

    def __getstate__(self):
        # Customize pickle because `self._bucket_`
        # (when not None) can't be pickled.
        # the `service_account.Credentials` class object can be pickled.
        return (
            self.bucket_name,
            self._quiet_multidownload,
        ), super().__getstate__()

    def __setstate__(self, data):
        (self.bucket_name, self._quiet_multidownload), z1 = data
        self._bucket_ = None
        self._blob_ = None
        self._lock_count = 0
        self._generation = -1
        return super().__setstate__(z1)

    def _bucket(self) -> storage.Bucket:
        """
        Return a Bucket object, via :meth:`client`.
        """
        if self._bucket_ is None:
            self._bucket_ = self._client().bucket(self.bucket_name)
        return self._bucket_

    def _blob(self) -> storage.Blob:
        """
        This constructs a Blob object irrespective of whether the blob
        exists in GCS.
        """
        if self._blob_ is None:
            self._blob_ = self._bucket().blob(self.blob_name)
        return self._blob_

    @overrides
    def as_uri(self) -> str:
        """
        Represent the path as a file URI, like 'gs://bucket-name/path/to/blob'.
        """
        return f"gs://{self.bucket_name}/{self._path.lstrip('/')}"

    @overrides
    def is_file(self) -> bool:
        """
        The result of this call is not cached, in case the object is modified anytime
        by other clients.
        """
        return self._blob().exists(self._client())

    @overrides
    def is_dir(self) -> bool:
        """
        If there is a dummy blob with name ``f"{self.name}/"``,
        this will return ``True``.
        This is the case after creating a "folder" on the GCP dashboard.
        In programatic use, it's recommended to avoid such situations so that
        ``is_dir()`` returns ``True`` if and only if there are blobs
        "under" the current path.
        """
        prefix = self.blob_name + "/"
        blobs = self._client().list_blobs(
            self._bucket(),
            prefix=prefix,
            max_results=1,
            page_size=1,
            fields="items(name),nextPageToken",
        )
        return len(list(blobs)) > 0

    @overrides
    def file_info(self) -> Optional[FileInfo]:
        """
        Return file info if the current path is a file;
        otherwise return ``None``.
        """
        b = self._blob()
        try:
            b.reload(client=self._client())
        except NotFound:
            return None
        return FileInfo(
            ctime=b.time_created.timestamp(),
            mtime=b.updated.timestamp(),
            time_created=b.time_created,
            time_modified=b.updated,
            size=b.size,  # bytes
            details=b._properties,
        )
        # If an existing file is written to again using `write_...`,
        # then its `ctime` and `mtime` are both updated.
        # My experiments showed that `ctime` and `mtime` are equal.

    @property
    @overrides
    def root(self) -> GcsBlobUpath:
        """
        Return a new path representing the root of the same bucket.
        """
        obj = self.__class__(
            bucket_name=self.bucket_name,
        )
        obj._bucket_ = self._bucket()
        return obj

    def _write_from_buffer(
        self, file_obj, *, overwrite=False, content_type=None, size=None
    ):
        if self._path == "/":
            raise UnsupportedOperation("can not write to root as a blob", self)

        try:
            self._blob().upload_from_file(
                file_obj,
                content_type=content_type,
                size=size,
                client=self._client(),
                if_generation_match=None if overwrite else 0,
            )
            # TODO: set "create_time", 'update_time" to be the same
            # as the source local file?
            # Blob objects has methods `_set_properties`, `_patch_property`,
            # `patch`.
        except PreconditionFailed:
            raise FileExistsError(self)

    def _write_bytes(self, data, **kwargs):
        b = BytesIO(data)
        b.seek(0)
        self._write_from_buffer(b, content_type="text/plain", size=len(data), **kwargs)

    @overrides
    def write_bytes(self, data: bytes | BufferedReader, *, overwrite=False):
        """
        Write bytes ``data`` to the current blob.

        In the usual case, ``data`` is bytes.
        The case where ``data`` is a ``BufferedReader`` object, such as an open file,
        is not well tested.
        """
        if isinstance(data, bytes):
            self._blob_rate_limit(self._write_bytes, data, overwrite=overwrite)
            return
        self._write_from_buffer(data, content_type="text/plain", overwrite=overwrite)

    def _read_into_buffer(self, file_obj):
        file_info = self.file_info()
        if not file_info:
            raise FileNotFoundError(self)
        file_size = file_info.size  # bytes
        if file_size <= LARGE_FILE_SIZE:
            try:
                self._blob().download_to_file(file_obj, client=self._client())
                return
            except NotFound:
                raise FileNotFoundError(self)

        def _download(client, blob, start, end):
            buffer = BytesIO()
            try:
                blob.download_to_file(buffer, client=client, start=start, end=end)
            except NotFound:
                raise FileNotFoundError(blob.name)
            # Both `start` and `end` are inclusive.
            # The very first `start` should be 0.
            buffer.seek(0)
            return buffer, end - start + 1

        def _do_download():
            client = self._client()
            blob = self._blob()
            k = 0
            p = 0
            while True:
                kk = min(k + LARGE_FILE_SIZE, file_size)
                p += 1
                yield (
                    _download,
                    (client, blob, k, kk - 1),
                    {},
                    f"part {p}",
                )
                k = kk
                if k >= file_size:
                    break

        if self._quiet_multidownload:
            desc = False
        else:
            desc = f"Downloading {self!r}"
        for buf, k in self._run_in_executor(_do_download(), desc):
            n = file_obj.write(buf.getbuffer())
            if n != k:
                raise BufferError(
                    f"expecting to read {k} bytes; actually read {n} bytes"
                )
            buf.close()

    @overrides
    def read_bytes(self) -> bytes:
        """
        Return the content of the current blob as bytes.
        """
        buffer = BytesIO()
        self._read_into_buffer(buffer)
        return buffer.getvalue()

    @opnieuw.retry(
        retry_on_exceptions=RETRY_WRITE_ON_EXCEPTIONS,
        max_calls_total=10,
        retry_window_after_first_call_in_seconds=100,
    )
    def _blob_rate_limit(self, func, *args, **kwargs):
        # `func_name` is a create/update/delete function.
        # Google imposes rate limiting on such requests.
        # According to Google doc, https://cloud.google.com/storage/quotas,
        #   There is a write limit to the same object name. This limit is once per second.
        return func(*args, **kwargs)

    @overrides
    def _copy_file(self, target: Upath, *, overwrite=False) -> None:
        if isinstance(target, GcsBlobUpath):
            # https://cloud.google.com/storage/docs/copying-renaming-moving-objects
            try:
                self._bucket().copy_blob(
                    self._blob(),
                    target._bucket(),
                    target.blob_name,
                    client=self._client(),
                    if_generation_match=None if overwrite else 0,
                )
            except NotFound:
                raise FileNotFoundError(self)
            except PreconditionFailed:
                raise FileExistsError(target)
        else:
            super()._copy_file(target, overwrite=overwrite)

    @overrides
    def download_file(self, target: LocalPathType, *, overwrite=False) -> None:
        """
        Download the content of the current blob to ``target``.
        """
        target = _resolve_local_path(target)
        if target.is_file():
            if not overwrite:
                raise FileExistsError(target)
            target.remove_file()
        elif target.is_dir():
            raise IsADirectoryError(target)

        os.makedirs(str(target.parent), exist_ok=True)
        try:
            with open(target, "wb") as file_obj:
                # If `target` is an existing directory,
                # will raise `IsADirectoryError`.
                self._read_into_buffer(file_obj)
            updated = self._blob().updated
            if updated is not None:
                mtime = updated.timestamp()
                os.utime(target, (mtime, mtime))
        except resumable_media.DataCorruption:
            target.remove_file()
            raise

    @overrides
    def upload_file(self, source: LocalPathType, *, overwrite=False) -> None:
        """
        Upload the content of ``source`` to the current blob.
        """
        source = _resolve_local_path(source)
        filename = str(source)
        content_type = self._blob()._get_content_type(None, filename=filename)

        if self.is_file():
            if not overwrite:
                raise FileExistsError(self)
            self.remove_file()

        def _upload():
            with open(filename, "rb") as file_obj:
                total_bytes = os.fstat(file_obj.fileno()).st_size
                self._write_from_buffer(
                    file_obj,
                    size=total_bytes,
                    content_type=content_type,
                    overwrite=overwrite,
                )

        self._blob_rate_limit(_upload)

    @overrides
    def iterdir(self) -> Iterator[GcsBlobUpath]:
        """
        Yield immediate children under the current dir.
        """
        # From Google doc:
        #
        # Lists all the blobs in the bucket that begin with the prefix.
        #
        # This can be used to list all blobs in a "folder", e.g. "public/".
        #
        # The delimiter argument can be used to restrict the results to only the
        # "files" in the given "folder". Without the delimiter, the entire tree under
        # the prefix is returned. For example, given these blobs:
        #
        #     a/1.txt
        #     a/b/2.txt
        #
        # If you specify prefix ='a/', without a delimiter, you'll get back:
        #
        #     a/1.txt
        #     a/b/2.txt
        #
        # However, if you specify prefix='a/' and delimiter='/', you'll get back
        # only the file directly under 'a/':
        #
        #     a/1.txt
        #
        # As part of the response, you'll also get back a blobs.prefixes entity
        # that lists the "subfolders" under `a/`:
        #
        #     a/b/
        #
        # Search "List the objects in a bucket using a prefix filter | Cloud Storage"
        #
        # You can "create folder" on the Google Cloud Storage dashboard. What it does
        # seems to create a dummy blob named with a '/' at the end and sized 0.
        # This case is handled in this function.

        prefix = self.blob_name + "/"
        k = len(prefix)
        blobs = self._client().list_blobs(self._bucket(), prefix=prefix, delimiter="/")
        for p in blobs:
            if p.name == prefix:
                # This happens if users has used the dashboard to "create a folder".
                # This seems to be a valid blob except its size is 0.
                # If user deliberately created a blob with this name and with content,
                # it's ignored. Do not use this name for a blob!
                continue
            obj = self / p.name[k:]  # files
            obj._blob_ = p
            yield obj
        for p in blobs.prefixes:
            yield self / p[k:].rstrip("/")  # "subdirectories"
            # If this is an "empty subfolder", it is counted but it can be
            # misleading. User should avoid creating such empty folders.

    @overrides
    def remove_dir(self, **kwargs) -> int:
        """
        Remove the current dir and all the content under it recursively.
        Return the number of blobs removed.
        """
        z = super().remove_dir(**kwargs)
        prefix = self.blob_name + "/"
        for p in self._client().list_blobs(self._bucket(), prefix=prefix):
            assert p.name.endswith("/")
            p.delete()
        return z

    @overrides
    def remove_file(self) -> None:
        """
        Remove the current blob.
        """
        try:
            self._blob_rate_limit(self._blob().delete, client=self._client())
            self._blob_ = None
        except NotFound:
            self._blob_ = None
            raise FileNotFoundError(self)

    @overrides
    def riterdir(self) -> Iterator[GcsBlobUpath]:
        """
        Yield all blobs recursively under the current dir.
        """
        prefix = self.blob_name + "/"
        k = len(prefix)
        for p in self._client().list_blobs(self._bucket(), prefix=prefix):
            if p.name.endswith("/"):
                # This can be an "empty folder"---better not create them!
                # Worse, this is an actual blob name---do not do this!
                continue
            obj = self / p.name[k:]
            obj._blob_ = p
            yield obj

    def _acquire_lease(self, *, timeout: int = None):
        # Note: `timeout = None` does not mean infinite wait.
        # It means a default wait time. If user wants longer wait,
        # just pass in a large number. Because user often associate
        # `timeout = None` with infinite wait, the default wait
        # is a long period.
        if self._path == "/":
            raise UnsupportedOperation("can not write to root as a blob", self)
        if timeout is None:
            timeout = 300  # seconds

        @opnieuw.retry(
            retry_on_exceptions=(
                *RETRY_WRITE_ON_EXCEPTIONS,
                PreconditionFailed,
                FileExistsError,
            ),
            max_calls_total=10,
            retry_window_after_first_call_in_seconds=timeout,
        )
        def _acquire_():
            self._write_bytes(b"0")
            self._generation = self._blob().generation

        t0 = time.perf_counter()
        try:
            _acquire_()
        except Exception as e:
            raise LockAcquireError(self, time.perf_counter() - t0) from e

    @contextlib.contextmanager
    @overrides
    def lock(self, *, timeout: int | float = None):
        """
        This implementation does not prevent the file from being deleted
        by other workers that does not use the 'if-generation-match' condition.
        It relies on the assumption that this blob
        is used solely in this locking logic.

        ``timeout`` is the wait time for acquiring the lease.
        If ``None``, the default value 300 seconds is used.
        """
        # References:
        # https://www.joyfulbikeshedding.com/blog/2021-05-19-robust-distributed-locking-algorithm-based-on-google-cloud-storage.html
        # https://cloud.google.com/storage/docs/generations-preconditions
        # https://cloud.google.com/storage/docs/gsutil/addlhelp/ObjectVersioningandConcurrencyControl

        if self._lock_count == 0:
            self._acquire_lease(timeout=timeout)
        self._lock_count += 1
        try:
            yield
        finally:
            self._lock_count -= 1
            if self._lock_count == 0:
                try:
                    self._blob_rate_limit(
                        self._blob().delete,
                        client=self._client(),
                        if_generation_match=self._generation,
                    )
                except Exception as e:
                    raise LockReleaseError(f"failed to delete lock file {self}") from e

    def open(self, mode="r", **kwargs):
        """
        Use this on a blob (not a "directory") as a context manager.
        See Google documentation.
        """
        return self._blob().open(mode, **kwargs)
