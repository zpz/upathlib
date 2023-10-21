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
from datetime import datetime, timezone
from io import BufferedReader, BytesIO, UnsupportedOperation

import google.auth
from google import resumable_media
from google.api_core import exceptions as api_exceptions
from google.api_core import retry as api_retry
from google.cloud import storage

# 60 seconds; this is the "connection timeout" to server.
# From my reading, it's the `timeout` parameter to `google.cloud.storage.client._connection.api_request`,
# that is, the `timeout` parameter to `google.cloud._http.JSONConnection.api_request`.
# `google.cloud` is repo python-cloud-core.
from google.cloud.storage.retry import (
    DEFAULT_RETRY,
    ConditionalRetryPolicy,
    is_generation_specified,
)
from typing_extensions import Self

from ._blob import BlobUpath, LocalPathType, _resolve_local_path
from ._upath import FileInfo, LockAcquireError, LockReleaseError, Upath
from ._util import MAX_THREADS, get_shared_thread_pool

# To see retry info, uncomment the following. The printout is typically not overwhelming.
# logging.getLogger('google.api_core.retry').setLevel(logging.DEBUG)

__all__ = ['GcsBlobUpath', 'get_google_auth']


logger = logging.getLogger(__name__)

# When downloading large files, there may be INFO logs from `google.resumable_media._helpers` about "No MD5 checksum was returned...".
# You may want to suppress that log by setting its level to WARNING.

# 67108864 = 256 * 1024 * 256 = 64 MB
MEGABYTES32 = 33554432
MEGABYTES64 = 67108864
LARGE_FILE_SIZE = MEGABYTES64


# Workaround:
# The availability of `Retry.with_timeout` is in a messy state
# between versions of `google-api-core`.
# `upathlib` requires a recent version which should be fine.
assert hasattr(DEFAULT_RETRY, 'with_timeout')


# DEFAULT_RETRY has default timeout 120 seconds.
DEFAULT_RETRY = DEFAULT_RETRY.with_timeout(300.0).with_delay(1.0, 10.0)
 # see `google.api_core.retry.exponential_sleep_generator`


DEFAULT_RETRY_ACQUIRE_LOCK = (
    DEFAULT_RETRY
    .with_predicate(
        lambda exc: DEFAULT_RETRY._predicate(exc) or isinstance(exc, FileExistsError)
    )
)


DEFAULT_RETRY_IF_GENERATION_SPECIFIED = ConditionalRetryPolicy(
    DEFAULT_RETRY,
    is_generation_specified,
    ['query_params'],
)


DEFAULT_RETRY_RELEASE_LOCK = ConditionalRetryPolicy(
    DEFAULT_RETRY,
    is_generation_specified,
    ['query_params'],
)


DEFAULT_RETRY_ON_RATE_LIMIT = (
    DEFAULT_RETRY
    .with_predicate(
        lambda exc: DEFAULT_RETRY._predicate(exc) or isinstance(exc, api_exceptions.TooManyRequests)
    )
)


def get_google_auth(
    project_id=None,
    credentials=None,
    *,
    scopes: list[str] = None,
    valid_for_seconds: int = 300,
):
    renewed = False
    if project_id is None or credentials is None:
        cred, pid = google.auth.default(
            scopes=scopes or ["https://www.googleapis.com/auth/cloud-platform"]
        )
        if credentials is None:
            credentials = cred
        if project_id is None:
            project_id = pid
        renewed = True

    if (
        not credentials.token
        or (credentials.expiry - datetime.utcnow()).total_seconds() < valid_for_seconds
    ):
        api_retry.Retry(
            predicate=api_retry.if_exception_type(
                google.auth.exceptions.RefreshError,
                google.auth.exceptions.TransportError,
            ),
        )(credentials.refresh)(google.auth.transport.requests.Request())
        # One check shows that this token expires in one hour.
        renewed = True

    if renewed:
        logger.debug("Google credentials refreshed")

    return project_id, credentials, renewed


class GcsBlobUpath(BlobUpath):
    """
    GcsBlobUpath implements the :class:`~upathlib.Upath` API for
    Google Cloud Storage using the package
    `google-cloud-storage <https://github.com/googleapis/python-storage/tree/main>`_.
    """

    _PROJECT_ID: str = None
    _CREDENTIALS: google.auth.credentials.Credentials = None
    _CLIENT: storage.Client = None
    # The `storage.Client` object is not pickle-able.
    # But if it is copied into another "forked" process, it will function properly.
    # Hence this is safe with multiprocessing, be it forked or spawned.
    # In a "spawned" process, this will start as None.

    _LOCK_EXPIRE_IN_SECONDS: int = 600
    # Things performed while holding a `lock` should finish within
    # this many seconds. If a worker tries but fails to acquire a lock on a file,
    # and finds the lock file has existed this long, it assumes the file
    # is "dead" because somehow the previous locking failed to delete the file properly,
    # and it will delete this lock file, and retry lock acquisition.
    #
    # Usually you don't need to customize this.

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
        cls._PROJECT_ID, cls._CREDENTIALS, renewed = get_google_auth(
            cls._PROJECT_ID, cls._CREDENTIALS
        )
        if cls._CLIENT is None or renewed:
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
        GcsBlobUpath('gs://backup/experiments/data/first.data')
        >>> GcsBlobUpath('/experiments/data/first.data', bucket_name='backup')
        GcsBlobUpath('gs://backup/experiments/data/first.data')
        >>> GcsBlobUpath('gs://backup/experiments/data/first.data')
        GcsBlobUpath('gs://backup/experiments/data/first.data')
        >>> GcsBlobUpath('gs://backup', 'experiments', 'data/first.data')
        GcsBlobUpath('gs://backup/experiments/data/first.data')
        """
        if bucket_name is None:
            # The first arg must be like
            #   'gs://bucket-name'
            # or
            #   'gs://bucket-name/path...'

            p0 = paths[0]
            assert p0.startswith("gs://"), p0
            p0 = p0[5:]
            k = p0.find("/")
            if k < 0:
                bucket_name = p0
                paths = paths[1:]
            else:
                bucket_name = p0[:k]
                p0 = p0[k:]
                paths = (p0, *paths[1:])

        super().__init__(*paths)
        assert bucket_name, bucket_name
        self.bucket_name = bucket_name
        self._bucket_ = None
        self._lock_count: int = 0
        self._generation = -1
        self._quiet_multidownload = True

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.as_uri()}')"

    def __str__(self) -> str:
        return self.as_uri()

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
        self._lock_count = 0
        self._generation = -1
        super().__setstate__(z1)

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
        return self._bucket().blob(self.blob_name)

    def as_uri(self) -> str:
        """
        Represent the path as a file URI, like 'gs://bucket-name/path/to/blob'.
        """
        return f"gs://{self.bucket_name}/{self._path.lstrip('/')}"

    def is_file(self) -> bool:
        """
        The result of this call is not cached, in case the object is modified anytime
        by other clients.
        """
        return self._blob().exists(self._client())

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

    def file_info(self, *, request_timeout=60) -> FileInfo | None:
        """
        Return file info if the current path is a file;
        otherwise return ``None``.

        ``request_timeout`` is the http request timeout, not "retry" timeout.
        The default is 60 (which is the value ``google.cloud.storage.constants._DEFAULT_TIMEOUT``).
        """
        b = self._blob()
        try:
            b.reload(
                client=self._client(),
                timeout=request_timeout,
                retry=DEFAULT_RETRY.with_timeout(max(120, request_timeout * 2)),
            )
        except api_exceptions.NotFound:
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
        self,
        file_obj,
        *,
        overwrite=False,
        content_type=None,
        size=None,
        timeout: float | None = None,
    ):
        if self._path == "/":
            raise UnsupportedOperation("can not write to root as a blob", self)

        try:
            if overwrite:
                if timeout is None:
                    retry = DEFAULT_RETRY_ON_RATE_LIMIT
                else:
                    retry = DEFAULT_RETRY_ON_RATE_LIMIT.with_timeout(timeout)
            else:
                if timeout is None:
                    retry = DEFAULT_RETRY_IF_GENERATION_SPECIFIED
                else:
                    retry = ConditionalRetryPolicy(
                        DEFAULT_RETRY.with_timeout(timeout),
                        is_generation_specified,
                        ['query_params'],
                    )
            self._blob().upload_from_file(
                file_obj,
                content_type=content_type,
                size=size,
                client=self._client(),
                if_generation_match=None if overwrite else 0,
                retry=retry,
            )
            # TODO: set "create_time", 'update_time" to be the same
            # as the source local file?
            # Blob objects has methods `_set_properties`, `_patch_property`,
            # `patch`.
        except api_exceptions.PreconditionFailed as e:
            raise FileExistsError(self) from e

    def _write_bytes(self, data, **kwargs):
        b = BytesIO(data)
        b.seek(0)
        self._write_from_buffer(b, content_type="text/plain", size=len(data), **kwargs)

    def write_bytes(self, data: bytes | BufferedReader, *, overwrite=False, timeout=None):
        """
        Write bytes ``data`` to the current blob.

        In the usual case, ``data`` is bytes.
        The case where ``data`` is a
        `io.BufferedReader <https://docs.python.org/3/library/io.html#io.BufferedReader>`_ object, such as an open file,
        is not well tested.
        """
        try:
            memoryview(data)
        except TypeError:  # file-like data
            self._write_from_buffer(
                data, content_type="text/plain", overwrite=overwrite, timeout=timeout,
            )
        else:  # bytes-like data
            self._write_bytes(data, overwrite=overwrite, timeout=timeout)

    def _multipart_download(self, blob_size, file_obj):
        client = self._client()
        blob = self._blob()

        def _download(start, end):
            # Both `start` and `end` are inclusive.
            # The very first `start` should be 0.
            buffer = BytesIO()
            target_size = end - start + 1
            current_size = 0
            while True:
                try:
                    blob.download_to_file(
                        buffer,
                        client=client,
                        start=start + current_size,
                        end=end,
                        raw_download=True,
                    )
                    # "checksum mismatch" errors were encountered when downloading Parquet files.
                    # `raw_download=True` seems to prevent that error.
                except api_exceptions.NotFound as e:
                    raise FileNotFoundError(blob.name) from e
                current_size = buffer.tell()
                if current_size >= target_size:
                    break
                # When a large number (say 10) of workers independently download
                # the same large blob, practice showed the number of bytes downloaded
                # may not match the number that was requested.
                # I did not see this mentioned in GCP doc.
            buffer.seek(0)
            if current_size > target_size:
                buffer.truncate(target_size)
            return buffer

        executor = get_shared_thread_pool("upathlib-gcs", MAX_THREADS - 2)
        k = 0
        tasks = []
        while True:
            kk = min(k + LARGE_FILE_SIZE, blob_size)
            t = executor.submit(_download, k, kk - 1)
            tasks.append(t)
            k = kk
            if k >= blob_size:
                break

        try:
            it = 0
            for t in tasks:
                buf = t.result()
                file_obj.write(buf.getbuffer())
                buf.close()
                it += 1
        except Exception:
            for tt in tasks[it:]:
                if not tt.done():
                    tt.cancel()
                    # This may not succeed, but there isn't a good way to
                    # guarantee cancellation here.
            raise

    def _read_into_buffer(self, file_obj, *, concurrent=True):
        file_info = self.file_info()
        if not file_info:
            raise FileNotFoundError(self)
        file_size = file_info.size  # bytes
        if file_size <= LARGE_FILE_SIZE or not concurrent:
            try:
                self._blob().download_to_file(
                    file_obj, client=self._client(), raw_download=True
                )
                # "checksum mismatch" errors were encountered when downloading Parquet files.
                # `raw_download=True` seems to prevent that error.
                return
            except api_exceptions.NotFound as e:
                raise FileNotFoundError(self) from e
        else:
            self._multipart_download(file_size, file_obj)

    def read_bytes(self, **kwargs) -> bytes:
        """
        Return the content of the current blob as bytes.
        """
        buffer = BytesIO()
        self._read_into_buffer(buffer, **kwargs)
        return buffer.getvalue()

    # Google imposes rate limiting on create/update/delete requests.
    # According to Google doc, https://cloud.google.com/storage/quotas,
    #   There is a limit on writes to the same object name. This limit is once per second.

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
            except api_exceptions.NotFound as e:
                raise FileNotFoundError(self) from e
            except api_exceptions.PreconditionFailed as e:
                raise FileExistsError(target) from e
        else:
            super()._copy_file(target, overwrite=overwrite)

    def download_file(
        self, target: LocalPathType, *, overwrite=False, concurrent=True
    ) -> None:
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
                self._read_into_buffer(file_obj, concurrent=concurrent)
            updated = self._blob().updated
            if updated is not None:
                mtime = updated.timestamp()
                os.utime(target, (mtime, mtime))
        except resumable_media.DataCorruption:
            target.remove_file()
            raise

    def upload_file(self, source: LocalPathType, *, overwrite=False, timeout=None) -> None:
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

        with open(filename, "rb") as file_obj:
            total_bytes = os.fstat(file_obj.fileno()).st_size
            self._write_from_buffer(
                file_obj,
                size=total_bytes,
                content_type=content_type,
                overwrite=overwrite,
                timeout=timeout,
            )

    def iterdir(self) -> Iterator[Self]:
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
            yield obj
        for p in blobs.prefixes:
            yield self / p[k:].rstrip("/")  # "subdirectories"
            # If this is an "empty subfolder", it is counted but it can be
            # misleading. User should avoid creating such empty folders.

    def remove_dir(self, **kwargs) -> int:
        """
        Remove the current dir and all the content under it recursively.
        Return the number of blobs removed.
        """
        z = super().remove_dir(**kwargs)
        prefix = self.blob_name + "/"
        for p in self._client().list_blobs(self._bucket(), prefix=prefix):
            assert p.name.endswith("/"), p.name
            p.delete()
        return z

    def remove_file(self) -> None:
        """
        Remove the current blob.
        """
        try:
            self._blob().delete(client=self._client())
        except api_exceptions.NotFound as e:
            raise FileNotFoundError(self) from e

    def riterdir(self) -> Iterator[Self]:
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
            yield obj

    def _acquire_lease(
        self,
        *,
        retry=DEFAULT_RETRY_ACQUIRE_LOCK,
    ):
        # Note: `timeout = None` does not mean infinite wait.
        # It means a default wait time. If user wants longer wait,
        # just pass in a large number. Because user often associate
        # `timeout = None` with infinite wait, the default wait
        # is a long period.
        if self._path == "/":
            raise UnsupportedOperation("can not write to root as a blob", self)
        timeout = retry.timeout

        t0 = time.perf_counter()
        try:
            retry(self._write_bytes)(b'0')
            # TODO: `self._write_bytes` itself has "retry", then it is "retried" again.
            # This seems to be wrong.
            self._generation = self._blob().generation
        except FileExistsError as e:
            finfo = self.file_info(request_timeout=timeout)
            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            file_age = (now - finfo.time_created).total_seconds()
            if file_age - timeout > self._LOCK_EXPIRE_IN_SECONDS:
                # If the file is old,
                # assume it is a dead file, that is, the last lock operation
                # somehow failed and did not delete the file.
                logger.warning(
                    "the locker file '%s' was created %d seconds ago; assuming it is dead and deleting it",
                    self,
                    int(file_age),
                )
                self._blob().delete(client=self._client())
                # If this fails, the exception will propagate, which is not LockAcquireError.
                # After deleting the file, try it again:
                self._acquire_lease(retry=retry)
            else:
                raise LockAcquireError(
                    f"waited on '{self}' for {time.perf_counter() - t0:.2f} seconds; gave up on {e!r}"
                ) from e
        except Exception as e:
            raise LockAcquireError(
                f"waited on '{self}' for {time.perf_counter() - t0:.2f} seconds; gave up on {e!r}"
            ) from e

    def _release_lease(self, *, retry=DEFAULT_RETRY_RELEASE_LOCK):
        # TODO:
        # once got "RemoteDisconnected" error after 0.01 seconds, which should have triggered retry
        # according to GCP code. I don't understand why retry apparently did not happen.
        t0 = time.perf_counter()
        try:
            self._blob().delete(
                client=self._client(),
                if_generation_match=self._generation,  # TODO: should we remove this condition?
                retry=retry,
            )
        except Exception as e:
            t1 = time.perf_counter()
            raise LockReleaseError(
                f"failed to delete lock file {self} after trying for {t1 - t0:.2f} seconds; gave up on {e!r}"
            ) from e

    @contextlib.contextmanager
    def lock(
        self,
        *,
        timeout=None,
    ):
        """
        This implementation does not prevent the file from being deleted
        by other workers that does not use the 'if-generation-match' condition.
        It relies on the assumption that this blob
        is used *cooperatively* solely in this locking logic.

        ``timeout`` is the wait time for acquiring or releasing the lease.
        If ``None``, the default value 300 seconds is used.
        If ``0``, exactly one attempt is made to acquire a lock.
        """
        # References:
        # https://www.joyfulbikeshedding.com/blog/2021-05-19-robust-distributed-locking-algorithm-based-on-google-cloud-storage.html
        # https://cloud.google.com/storage/docs/generations-preconditions
        # https://cloud.google.com/storage/docs/gsutil/addlhelp/ObjectVersioningandConcurrencyControl
        if timeout is None:
            acquire_retry = DEFAULT_RETRY_ACQUIRE_LOCK
        else:
            acquire_retry = DEFAULT_RETRY_ACQUIRE_LOCK.with_timeout(timeout)
        if timeout is None:
            release_retry = DEFAULT_RETRY_RELEASE_LOCK
        else:
            release_retry = ConditionalRetryPolicy(
                DEFAULT_RETRY.with_timeout(timeout),
                is_generation_specified,
                ['query_params'],
            )

        if self._lock_count == 0:
            self._acquire_lease(retry=acquire_retry)
        self._lock_count += 1
        try:
            yield
        finally:
            self._lock_count -= 1
            if self._lock_count == 0:
                self._release_lease(retry=release_retry)

    def open(self, mode="r", **kwargs):
        """
        Use this on a blob (not a "directory") as a context manager.
        See Google documentation.
        """
        return self._blob().open(mode, **kwargs)
