from __future__ import annotations
# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import contextlib
import logging
import os
import random
import socket
import time
import urllib3
from io import BufferedReader, UnsupportedOperation

import requests
from google.oauth2 import service_account
from google.cloud import storage
from google.api_core.exceptions import (
    NotFound, PreconditionFailed, TooManyRequests)
from overrides import overrides

from ._upath import FileInfo, Upath, LockAcquisitionTimeoutError
from ._blob import BlobUpath
from ._local import LocalUpath

logger = logging.getLogger(__name__)


class Backoff:
    def __init__(self, basetime=1):
        self._basetime = basetime
        self.retries = 0

    def sleep(self):
        t = self._basetime * 2 ** self.retries + random.uniform(0, 1)
        time.sleep(t)
        self.retries += 1


class GcpBlobUpath(BlobUpath):
    def __init__(self, *paths: str, bucket_name: str, account_info: dict):
        super().__init__(*paths)
        self._bucket_name = bucket_name
        self._account_info = account_info
        self._client = None
        self._bucket = None
        self._blob = None
        self._lock_count: int = 0
        self._generation = -1
        self._metageneration = -1

    def __repr__(self) -> str:
        return "{}('{}', bucket_name='{}')".format(
            self.__class__.__name__, self._path, self._bucket_name
        )

    def __str__(self) -> str:
        return f"{self._bucket_name}://{self._path}"

    def __eq__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._bucket_name != self._bucket_name:
            return NotImplemented
        return self._path == other._path

    def __lt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._bucket_name != self._bucket_name:
            return NotImplemented
        return self._path < other._path

    def __le__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._bucket_name != self._bucket_name:
            return NotImplemented
        return self._path <= other._path

    def __gt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._bucket_name != self._bucket_name:
            return NotImplemented
        return self._path > other._path

    def __ge__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._bucket_name != self._bucket_name:
            return NotImplemented
        return self._path >= other._path

    def __getstate__(self):
        # Customize pickle because `self._client` and `self._bucket`
        # (when not None) can't be pickled.
        return {
            '_path': self._path,
            '_bucket_name': self._bucket_name,
            '_account_info': self._account_info,
        }

    def __setstate__(self, data):
        self._path = data['_path']
        self._bucket_name = data['_bucket_name']
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
            self._bucket = self.client.bucket(self._bucket_name)
            # self._bucket = self.client.get_bucket(self._bucket_name)
        return self._bucket

    @property
    def bucket_name(self):
        return self._bucket_name

    def blob(self, **kwargs):
        if not kwargs:
            if self._blob is None:
                self._blob = self.bucket.blob(self.blob_name)
            return self._blob
        return self.bucket.blob(self.blob_name, **kwargs)

    @overrides
    def _copy_file(self, target: GcpBlobUpath) -> None:
        # https://cloud.google.com/storage/docs/copying-renaming-moving-objects
        self.bucket.copy_blob(
            self.blob(), target.bucket, target.blob_name
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
        self.blob().download_to_filename(str(target))
        # TODO: look into `retry`.

    @overrides
    def file_info(self):
        b = self.bucket.get_blob(self.blob_name)
        if b is not None:
            return FileInfo(
                ctime=b.time_created.timestamp(),
                mtime=b.updated.timestamp(),
                time_created=b.time_created,
                time_modified=b.updated,
                size=b.size,
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
        self.blob().upload_from_filename(str(source))
        # TODO: look into `retry`.

    @overrides
    def is_file(self) -> bool:
        return self.blob().exists()

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
        # for p in self.client.list_blobs(self.bucket, prefix=prefix, delimiter='/'):
        #     obj = self / p.name[k:]  # "files"
        #     obj._blob = p
        #     yield obj
        # for page in self.client.list_blobs(self.bucket, prefix=prefix, delimiter='/').pages:
        #     for p in page.prefixes:
        #         yield self / p[k:].rstrip('/')  # "subdirectories"
        blobs = self.client.list_blobs(self.bucket, prefix=prefix, delimiter='/')
        for p in blobs:
            obj = self / p.name[k:]  # files
            obj._blob = p
            yield obj
        for p in blobs.prefixes:
            yield self / p[k:].rstrip('/')  # "subdirectories"

    def _blob_retry(self, func_name, *args, max_tries=5, **kwargs):
        # `func_name` is the name of a blob method.
        sleeper = Backoff()
        while True:
            try:
                return getattr(self.blob(), func_name)(*args, **kwargs)
            except (requests.exceptions.ReadTimeout,
                    urllib3.exceptions.ReadTimeoutError,
                    socket.timeout,
                    ) as e:
                if sleeper.retries > max_tries:
                    raise
                logger.info("retrying %r: %r", func_name, e)
                sleeper.sleep()
                # My hypothesis is that sometimes timeout happens
                # because the cached `client`, `bucket`, `blob`
                # have become stale, hence should be recreated.
                self._client = None
                self._bucket = None
                self._blob = None

    def _blob_rate_limit(self, func_name, *args, **kwargs):
        # `func_name` is the name of a create/update/delete function.
        sleeper = Backoff()
        while True:
            try:
                return self._blob_retry(func_name, *args, **kwargs)
            except (TooManyRequests,
                    urllib3.exceptions.SSLError,
                    requests.exceptions.SSLError) as e:
                if sleeper.retries >= 5:
                    raise
                logger.info("retrying %r: %r", func_name, e)
                sleeper.sleep()

    def _acquire_lease(self, timeout: int = None):
        if self._path == '/':
            raise UnsupportedOperation("can not write to root as a blob", self)
        if timeout is None:
            timeout = 600
        b = self.blob()
        t0 = time.perf_counter()
        n = 0
        while True:
            try:
                b.upload_from_string(
                    b'0', if_generation_match=0, timeout=10, retry=None)
                b.cache_control = 'no-store'
                b.patch()
                self._generation = b.generation
                self._metageneration = b.metageneration
                return
            except (PreconditionFailed, TooManyRequests):
                pass
            except (urllib3.exceptions.SSLError,
                    requests.exceptions.SSLError,
                    requests.exceptions.ConnectionError) as e:
                n += 1
                if n % 10 == 0:
                    logger.warning('trying to acquire lock on %s: %s', self, e)
            except Exception as e:
                print('\n\nin GcpBlobUpath lock acquire', flush=True)
                print(e, flush=True)
                print(e.__class__, flush=True)
                print('\n\n', flush=True)
                raise

            t1 = time.perf_counter()
            if t1 - t0 >= timeout:
                raise LockAcquisitionTimeoutError(self, t1 - t0)
            time.sleep(random.uniform(0.02, 0.2))

    @contextlib.contextmanager
    @overrides
    def lock(self, *, timeout=None):
        # References:
        # https://www.joyfulbikeshedding.com/blog/2021-05-19-robust-distributed-locking-algorithm-based-on-google-cloud-storage.html
        # https://cloud.google.com/storage/docs/generations-preconditions
        # https://cloud.google.com/storage/docs/gsutil/addlhelp/ObjectVersioningandConcurrencyControl

        # TODO: this implementation needs enhancements.
        # I did not get the `generation`, `if-generation-match` work.
        # This implementation does not prevent the file from being deleted
        # by other workers. It relies on the assumption that this blob
        # is used solely in this locking logic.

        if self._lock_count == 0:
            self._acquire_lease(timeout)
        self._lock_count += 1
        try:
            yield
        finally:
            self._lock_count -= 1
            if self._lock_count == 0:
                try:
                    self._blob_rate_limit(
                        'delete',
                        if_generation_match=self._generation,
                        if_metageneration_match=self._metageneration,
                    )
                except Exception as e:
                    logger.error(e)

    @overrides
    def read_bytes(self) -> bytes:
        try:
            return self._blob_retry('download_as_bytes')
        except NotFound as e:
            raise FileNotFoundError(self) from e

    @overrides
    def remove_file(self) -> int:
        try:
            self._blob_rate_limit('delete')
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
        obj = self.__class__(*paths, bucket_name=self._bucket_name,
                             account_info=self._account_info)
        obj._client = self._client
        obj._bucket = self._bucket
        obj._blob = None
        return obj

    @overrides
    def write_bytes(self, data, *, overwrite=False) -> int:
        if self._path == '/':
            raise UnsupportedOperation("can not write to root as a blob", self)
        if isinstance(data, BufferedReader):
            data = data.read()
        nbytes = len(data)

        if overwrite:
            self._blob_rate_limit('upload_from_string', data, retry=None)
            # this will overwrite existing content.
        else:
            try:
                self._blob_rate_limit(
                    'upload_from_string',
                    data,
                    if_generation_match=0)
            except PreconditionFailed:
                raise FileExistsError(self)
        return nbytes
