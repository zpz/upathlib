from __future__ import annotations
# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import contextlib
import logging
import os
# import socket
# import urllib3
from io import BufferedReader, UnsupportedOperation, BytesIO
from typing import Union

# import requests
from google import resumable_media
from google.oauth2 import service_account
from google.cloud import storage
from google.api_core.exceptions import (
    NotFound, PreconditionFailed, TooManyRequests)
from overrides import overrides

from ._upath import FileInfo, Upath, LockAcquisitionTimeoutError
from ._blob import BlobUpath
from ._local import LocalUpath
from ._util import Backoff


logger = logging.getLogger(__name__)


# 67108864 = 256 * 1024 * 256 = 64 MB
MEGABYTES32 = 33554432
MEGABYTES64 = 67108864


class GcpBlobUpath(BlobUpath):
    def __init__(self, *paths: str, bucket_name: str, account_info: dict):
        '''
        `account_info`: a dict with these elements:
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
        '''
        super().__init__(*paths)
        self.bucket_name = bucket_name
        self._account_info = account_info
        self._client = None
        self._bucket = None
        self._blob = None
        self._lock_count: int = 0
        self._generation = -1

    def __repr__(self) -> str:
        return "{}('{}', bucket_name='{}')".format(
            self.__class__.__name__, self._path, self.bucket_name
        )

    def __str__(self) -> str:
        return f"{self.bucket_name}://{self._path}"

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
        return {
            '_path': self._path,
            'bucket_name': self.bucket_name,
            '_account_info': self._account_info,
        }

    def __setstate__(self, data):
        self._path = data['_path']
        self.bucket_name = data['bucket_name']
        self._account_info = data['_account_info']
        self._client = None
        self._bucket = None
        self._blob = None
        self._lock_count = 0

    @property
    def client(self):
        if self._client is None:
            gcp_cred = service_account.Credentials.from_service_account_info(
                self._account_info)
            self._client = storage.Client(
                project=self._account_info['project_id'],
                credentials=gcp_cred,
            )
        return self._client

    @property
    def bucket(self):
        if self._bucket is None:
            self._bucket = self.client.bucket(self.bucket_name)
            # self._bucket = self.client.get_bucket(self.bucket_name)
        return self._bucket

    def blob(self):
        '''
        This constructs a Blob object irrespecitive of whether the blob
        exists in cloud storage.
        '''
        if self._blob is None:
            self._blob = self.bucket.blob(self.blob_name)
        return self._blob

    def get_blob(self):
        '''
        While `.blob` simply constructs a Blob object,
        `.get_blob` makes network calls to refresh properties
        of the object in cloud storage. If the blob does not exist,
        return `None`.
        '''
        b = self.blob()
        try:
            b.reload(client=self.client)
            return b
        except NotFound:
            return None

    # def _blob_retry(self, func_name, *args, max_tries=3, **kwargs):
    #     # `func_name` is the name of a blob method.
    #     sleeper = Backoff(0.1)
    #     while True:
    #         try:
    #             return getattr(self.blob(), func_name)(*args, **kwargs)
    #         except (requests.exceptions.ReadTimeout,
    #                 urllib3.exceptions.ReadTimeoutError,
    #                 socket.timeout,
    #                 urllib3.exceptions.SSLError,
    #                 requests.exceptions.SSLError,
    #                 requests.exceptions.ConnectionError,
    #                 ) as e:
    #             if sleeper.retries >= max_tries:
    #                 raise
    #             logger.info("retrying %r on error: %r", func_name, e)
    #             sleeper.sleep()
    #             # My hypothesis is that sometimes timeout happens
    #             # because the cached `client`, `bucket`, `blob`
    #             # have become stale, hence should be recreated.
    #             self._client = None
    #             self._bucket = None
    #             self._blob = None

    def _blob_rate_limit(self, func, *args, max_retries=5, **kwargs):
        # `func_name` is the name of a create/update/delete function.
        sleeper = Backoff(0.2)
        while True:
            try:
                return func(*args, **kwargs)
            except TooManyRequests as e:
                if sleeper.retries >= max_retries:
                    raise
                logger.info("retrying %r on error: %r", func, e)
                sleeper.sleep()

    @overrides
    def _copy_file(self, target: GcpBlobUpath) -> None:
        # https://cloud.google.com/storage/docs/copying-renaming-moving-objects
        self.bucket.copy_blob(
            self.blob(), target.bucket, target.blob_name, client=self.client,
        )

    @overrides
    def export_dir(self, target, **kwargs) -> int:
        _ = self.client
        _ = self.bucket
        return super().export_dir(target, **kwargs)

    @overrides
    def _export_file(self, target: Upath) -> None:
        if not isinstance(target, LocalUpath):
            return super()._export_file(target)
        os.makedirs(str(target.parent), exist_ok=True)
        try:
            with open(str(target), 'wb') as file_obj:
                self._read_into_buffer(file_obj)
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
    def import_dir(self, source, **kwargs) -> int:
        _ = self.client
        _ = self.bucket
        return super().import_dir(source, **kwargs)

    @overrides
    def _import_file(self, source: Upath) -> None:
        if not isinstance(source, LocalUpath):
            return super()._import_file(source)

        def _upload(filename, **kwargs):
            with open(filename, 'rb') as file_obj:
                total_bytes = os.fstat(file_obj.fileno()).st_size
                self._write_from_buffer(file_obj, size=total_bytes, **kwargs)

        filename = str(source)
        content_type = self.blob()._get_content_type(None, filename=filename)
        self._blob_rate_limit(_upload, filename, content_type=content_type)

    @overrides
    def is_file(self) -> bool:
        # This is not cached, in case the object is modified anytime
        # by other clients.
        return self.blob().exists(self.client)

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

        prefix = self.blob_name + '/'
        k = len(prefix)
        blobs = self.client.list_blobs(self.bucket, prefix=prefix, delimiter='/')
        for p in blobs:
            obj = self / p.name[k:]  # files
            obj._blob = p
            yield obj
        for p in blobs.prefixes:
            yield self / p[k:].rstrip('/')  # "subdirectories"

    def _acquire_lease(self, *, timeout: int = None):
        if self._path == '/':
            raise UnsupportedOperation("can not write to root as a blob", self)
        if timeout is None:
            timeout = 600  # seconds
        sleeper = Backoff(0.1)
        while True:
            try:
                # b.upload_from_string(b'0', if_generation_match=0)
                # b.cache_control = 'no-store'
                # b.patch()
                self._blob_rate_limit(self._write_bytes, b'0')
                self._generation = self.blob().generation
                return
            except (PreconditionFailed, FileExistsError):
                if (t := sleeper.time_elapsed) >= timeout:
                    raise LockAcquisitionTimeoutError(self, t)
                sleeper.sleep()

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
                        max_retries=10,
                    )
                except Exception as e:
                    logger.error(e)

    def open(self, mode='r', **kwargs):
        '''
        Use this on a blob (not a "directory") as a context manager.
        See Google documentation.
        '''
        return self.blob().open(mode, **kwargs)

    def _read_into_buffer(self, file_obj):
        file_info = self.file_info()
        if not file_info:
            raise FileNotFoundError(self)
        file_size = file_info.size  # bytes
        if file_size <= MEGABYTES32:
            self.blob().download_to_file(file_obj, client=self.client)
            return

        def _download(client, blob, start, end):
            buffer = BytesIO()
            blob.download_to_file(buffer, client=client, start=start, end=end)
            buffer.seek(0)
            return buffer, end - start

        def _do_download():
            client = self.client
            blob = self.blob()
            name = self.name
            k = 0
            p = 0
            while True:
                kk = min(k + MEGABYTES32, file_size)
                p += 1
                yield (_download, (client, blob, k, kk - 1), {}, f"{name}: part {p}")
                k = kk
                if k >= file_size:
                    break

        for buf, k in self._run_in_executor(_do_download()):
            n = buf.readinto(file_obj)
            if n != k:
                raise BufferError(f"expecting to read {k} bytes; actually read {n} bytes")
            buf.close()

    @overrides
    def read_bytes(self) -> bytes:
        buffer = BytesIO()
        self._read_into_buffer(buffer)
        return buffer.getvalue()

    @overrides
    def remove_file(self) -> int:
        try:
            self._blob_rate_limit(self.blob().delete, client=self.client)
            self._blob = None
            return 1
        except NotFound:
            self._blob = None
            return 0

    @overrides
    def riterdir(self):
        prefix = self.blob_name + '/'
        k = len(prefix)
        for p in self.client.list_blobs(self.bucket, prefix=prefix):
            obj = self / p.name[k:]
            obj._blob = p
            yield obj

    @overrides
    def with_path(self, *paths: str):
        obj = self.__class__(*paths, bucket_name=self.bucket_name,
                             account_info=self._account_info)
        obj._client = self._client
        obj._bucket = self._bucket
        return obj

    def _write_from_buffer(self, file_obj, *, overwrite=False, content_type=None, size=None):
        if self._path == '/':
            raise UnsupportedOperation("can not write to root as a blob", self)

        if overwrite:
            self.blob().upload_from_file(
                file_obj,
                content_type=content_type,
                size=size,
                client=self.client,
            )
            # this will overwrite existing content if any.
            return

        try:
            self.blob().upload_from_file(
                file_obj,
                content_type=content_type,
                size=size,
                client=self.client,
                if_generation_match=0,
            )
        except PreconditionFailed:
            raise FileExistsError(self)

    def _write_bytes(self, data, **kwargs):
        b = BytesIO(data)
        b.seek(0)
        self._write_from_buffer(b, content_type='text/plain', size=len(data), **kwargs)

    @overrides
    def write_bytes(self, data: Union[bytes, BufferedReader], *, overwrite=False):
        if isinstance(data, bytes):
            self._blob_rate_limit(self._write_bytes, data, overwrite=overwrite)
            return
        self._write_from_buffer(data, content_type='text/plain', overwrite=overwrite)
