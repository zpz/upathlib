from __future__ import annotations

# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import contextlib
import logging
import os
import time
from io import BufferedReader, UnsupportedOperation, BytesIO
from typing import Union

import opnieuw
import requests
from google import resumable_media
from google.oauth2 import service_account
from google.cloud import storage
from google.api_core.exceptions import (
    NotFound,
    PreconditionFailed,
    TooManyRequests,
    GatewayTimeout,
    ServiceUnavailable,
)
from overrides import overrides

from ._upath import FileInfo, Upath, LockAcquisitionTimeoutError
from ._blob import BlobUpath
from ._local import LocalUpath


logger = logging.getLogger(__name__)


# 67108864 = 256 * 1024 * 256 = 64 MB
MEGABYTES32 = 33554432
MEGABYTES64 = 67108864


class GcpBlobUpath(BlobUpath):
    def __init__(
        self,
        *paths: str,
        bucket_name: str = None,
        project_id: str = None,
        credentials: service_account.Credentials = None,
    ):
        """
        If you have GCP account_info in a dict with these elements:

            'type': 'service_account',
            'project_id':
            'private_key_id':
            'private_key':
                '-----BEGIN PRIVATE KEY-----\n'
                + private_key.encode('latin1').decode('unicode_escape')
                + '\n-----END PRIVATE KEY-----\n',
            'client_email':
            'client_id':
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
            'client_x509_cert_url': f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}"

        then `credentials` are obtained by

            google.oauth2.service_account.Credentials.from_service_account_info(account_info)

        If this code is used on a GCP machine, already in the context of an account/project, then
        `project_id` and `credentials` are not needed (but not harmful either).

        TODO: check out github repo `googleapis/python-cloud-core` to see whether there is
        a way or a need to infer these values. In particular, see class `google.cloud.client.ClientWithProject`.
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
        self._project_id = project_id
        self._credentials = credentials
        self._client = None
        self._bucket = None
        self._blob = None
        self._lock_count: int = 0
        self._generation = -1

    def __repr__(self) -> str:
        return "{}('gs://{}/{}')".format(
            self.__class__.__name__,
            self.bucket_name,
            self._path.lstrip("/"),
        )

    def __str__(self) -> str:
        return self.__repr__()

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
        # Customize pickle because `self._client` and `self._bucket`
        # (when not None) can't be pickled.
        # the `service_account.Credentials` class object can be pickled.
        return {
            "_path": self._path,
            "bucket_name": self.bucket_name,
            "_project_id": self._project_id,
            "_credentials": self._credentials,
        }

    def __setstate__(self, data):
        self._path = data["_path"]
        self.bucket_name = data["bucket_name"]
        self._project_id = data["_project_id"]
        self._credentials = data["_credentials"]
        self._client = None
        self._bucket = None
        self._blob = None
        self._lock_count = 0

    @property
    def client(self):
        if self._client is None:
            self._client = storage.Client(
                project=self._project_id,
                credentials=self._credentials,
            )
        return self._client

    @property
    def bucket(self):
        if self._bucket is None:
            self._bucket = self.client.bucket(self.bucket_name)
            # self._bucket = self.client.get_bucket(self.bucket_name)
        return self._bucket

    def blob(self):
        """
        This constructs a Blob object irrespecitive of whether the blob
        exists in cloud storage.
        """
        if self._blob is None:
            self._blob = self.bucket.blob(self.blob_name)
        return self._blob

    def get_blob(self):
        """
        While `.blob` simply constructs a Blob object,
        `.get_blob` makes network calls to refresh properties
        of the object in cloud storage. If the blob does not exist,
        return `None`.
        """
        b = self.blob()
        try:
            b.reload(client=self.client)
            return b
        except NotFound:
            return None

    @opnieuw.retry(
        retry_on_exceptions=(
            TooManyRequests,
            GatewayTimeout,
            ServiceUnavailable,
            requests.ReadTimeout,
        ),
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
    def _copy_file(self, target: GcpBlobUpath, *, overwrite=False) -> None:
        # https://cloud.google.com/storage/docs/copying-renaming-moving-objects
        try:
            self.bucket.copy_blob(
                self.blob(),
                target.bucket,
                target.blob_name,
                client=self.client,
                if_generation_match=None if overwrite else 0,
            )
        except NotFound:
            raise FileNotFoundError(self)
        except PreconditionFailed:
            raise FileExistsError(target)

    @overrides
    def export_file(self, target: Upath, *, overwrite=False) -> None:
        if not isinstance(target, LocalUpath):
            return super().export_file(target, overwrite=overwrite)

        # File download.

        if not overwrite and target.is_file():
            raise FileExistsError(target)
        os.makedirs(str(target.parent), exist_ok=True)
        try:
            with open(target.localpath, "wb") as file_obj:
                # If `target` is an existing directory,
                # will raise `IsADirectoryError`.
                self._read_into_buffer(file_obj)
            updated = self.blob().updated
            if updated is not None:
                mtime = updated.timestamp()
                os.utime(target.localpath, (mtime, mtime))
        except resumable_media.DataCorruption:
            target.remove_file()
            raise

    @overrides
    def file_info(self):
        b = self.get_blob()
        if not b:
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

    @overrides
    def import_file(self, source: Upath, *, overwrite=False) -> None:
        if not isinstance(source, LocalUpath):
            return super().import_file(source, overwrite=overwrite)

        # File upload.

        filename = str(source)
        content_type = self.blob()._get_content_type(None, filename=filename)

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
    def is_file(self) -> bool:
        # This is not cached, in case the object is modified anytime
        # by other clients.
        return self.blob().exists(self.client)

    @overrides
    def is_dir(self) -> bool:
        prefix = self.blob_name + "/"
        blobs = self.client.list_blobs(self.bucket, prefix=prefix,
                max_results=1, page_size=1, fields='items(name),nextPageToken')
        return len(list(blobs)) > 0

    @overrides
    def iterdir(self):
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

        prefix = self.blob_name + "/"
        k = len(prefix)
        blobs = self.client.list_blobs(self.bucket, prefix=prefix, delimiter="/")
        for p in blobs:
            obj = self / p.name[k:]  # files
            obj._blob = p
            yield obj
        for p in blobs.prefixes:
            yield self / p[k:].rstrip("/")  # "subdirectories"

    def _acquire_lease(self, *, timeout: int = None):
        # Note: `timeout = None` does not mean infinite wait.
        # It means a default wait time. If user wants longer wait,
        # just pass in a large number. Because user often associate
        # `timeout = None` with infinite wait, the default wait
        # is a long period.
        if self._path == "/":
            raise UnsupportedOperation("can not write to root as a blob", self)
        if timeout is None:
            timeout = 3600  # seconds

        @opnieuw.retry(
            retry_on_exceptions=(PreconditionFailed, FileExistsError),
            max_calls_total=10,
            retry_window_after_first_call_in_seconds=timeout,
        )
        def _acquire_():
            self._blob_rate_limit(self._write_bytes, b"0")
            self._generation = self.blob().generation

        t0 = time.perf_counter()
        try:
            _acquire_()
        except Exception as e:
            raise LockAcquisitionTimeoutError(self, time.perf_counter() - t0) from e

    @contextlib.contextmanager
    @overrides
    def lock(self, *, timeout=None):
        # References:
        # https://www.joyfulbikeshedding.com/blog/2021-05-19-robust-distributed-locking-algorithm-based-on-google-cloud-storage.html
        # https://cloud.google.com/storage/docs/generations-preconditions
        # https://cloud.google.com/storage/docs/gsutil/addlhelp/ObjectVersioningandConcurrencyControl

        # This implementation does not prevent the file from being deleted
        # by other workers that does not use the 'if-generation-match' condition.
        # It relies on the assumption that this blob
        # is used solely in this locking logic.

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
                        self.blob().delete,
                        client=self.client,
                        if_generation_match=self._generation,
                    )
                except Exception as e:
                    logger.error(e)

    def open(self, mode="r", **kwargs):
        """
        Use this on a blob (not a "directory") as a context manager.
        See Google documentation.
        """
        return self.blob().open(mode, **kwargs)

    def _read_into_buffer(self, file_obj):
        file_info = self.file_info()
        if not file_info:
            raise FileNotFoundError(self)
        file_size = file_info.size  # bytes
        if file_size <= MEGABYTES32:
            try:
                self.blob().download_to_file(file_obj, client=self.client)
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
            client = self.client
            blob = self.blob()
            k = 0
            p = 0
            while True:
                kk = min(k + MEGABYTES32, file_size)
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

        for buf, k in self._run_in_executor(_do_download(), f"Downloading {self!r}"):
            n = file_obj.write(buf.getbuffer())
            if n != k:
                raise BufferError(
                    f"expecting to read {k} bytes; actually read {n} bytes"
                )
            buf.close()

    @overrides
    def read_bytes(self) -> bytes:
        buffer = BytesIO()
        self._read_into_buffer(buffer)
        return buffer.getvalue()

    @overrides
    def remove_file(self):
        try:
            self._blob_rate_limit(self.blob().delete, client=self.client)
            self._blob = None
        except NotFound:
            self._blob = None
            raise FileNotFoundError(self)

    @overrides
    def riterdir(self):
        prefix = self.blob_name + "/"
        k = len(prefix)
        for p in self.client.list_blobs(self.bucket, prefix=prefix):
            obj = self / p.name[k:]
            obj._blob = p
            yield obj

    @overrides
    def with_path(self, *paths: str):
        obj = self.__class__(
            *paths,
            bucket_name=self.bucket_name,
            project_id=self._project_id,
            credentials=self._credentials,
        )
        obj._client = self._client
        obj._bucket = self._bucket
        return obj

    def _write_from_buffer(
        self, file_obj, *, overwrite=False, content_type=None, size=None
    ):
        if self._path == "/":
            raise UnsupportedOperation("can not write to root as a blob", self)

        try:
            self.blob().upload_from_file(
                file_obj,
                content_type=content_type,
                size=size,
                client=self.client,
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
    def write_bytes(self, data: Union[bytes, BufferedReader], *, overwrite=False):
        if isinstance(data, bytes):
            self._blob_rate_limit(self._write_bytes, data, overwrite=overwrite)
            return
        self._write_from_buffer(data, content_type="text/plain", overwrite=overwrite)
