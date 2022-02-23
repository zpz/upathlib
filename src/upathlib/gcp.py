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
from google.oauth2 import service_account  # type: ignore
from google.cloud import storage  # type: ignore
from google.api_core.exceptions import (  # type: ignore
    NotFound, PreconditionFailed, TooManyRequests)  # type: ignore

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

    def _copy_file(self, target: GcpBlobUpath):
        # https://cloud.google.com/storage/docs/copying-renaming-moving-objects
        self.bucket.copy_blob(
            self.blob(), target.bucket, target.blob_name
        )

    def export_dir(self, target, **kwargs):
        _ = self.client
        _ = self.bucket
        return super().export_dir(target, **kwargs)

    def _export_file(self, target: Upath):
        if not isinstance(target, LocalUpath):
            return super()._export_file(target)
        os.makedirs(str(target.parent), exist_ok=True)
        self.blob().download_to_filename(str(target))
        # TODO: look into `retry`.

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

    def import_dir(self, source, **kwargs):
        _ = self.client
        _ = self.bucket
        return super().import_dir(source, **kwargs)

    def _import_file(self, source: Upath):
        if not isinstance(source, LocalUpath):
            return super()._import_file(source)
        self.blob().upload_from_filename(str(source))
        # TODO: look into `retry`.

    def is_file(self) -> bool:
        return self.blob().exists()

    def iterdir(self):
        prefix = self.blob_name + '/'
        k = len(prefix)
        for p in self.client.list_blobs(self.bucket, prefix=prefix, delimiter='/'):
            obj = self / p.name[k:]  # "files"
            obj._blob = p
            yield obj
        for page in self.client.list_blobs(self.bucket, prefix=prefix, delimiter='/').pages:
            for p in page.prefixes:
                yield self / p[k:].rstrip('/')  # "subdirectories"

    def _rate_limit(self, func, *args, **kwargs):
        # `func` is a create/update/delete function.
        sleeper = Backoff()
        while True:
            try:
                return func(*args, **kwargs)
            except (TooManyRequests,
                    urllib3.exceptions.SSLError,
                    requests.exceptions.SSLError) as e:
                sleeper.sleep()
                if sleeper.retries >= 5:
                    raise
                logger.info('retrying %r: %r', func, e)

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
                    self._rate_limit(self.blob().delete,
                                     if_generation_match=self._generation,
                                     if_metageneration_match=self._metageneration,
                                     )
                except Exception as e:
                    logger.error(e)

    def read_bytes(self):
        sleeper = Backoff()
        while True:
            try:
                return self.blob().download_as_bytes()
            except (requests.exceptions.ReadTimeout,
                    urllib3.exceptions.ReadTimeoutError,
                    socket.timeout,
                    ):
                if sleeper.retries > 3:
                    raise
                sleeper.sleep()
                # My hypothesis is that sometimes timeout happens
                # because the cached `client`, `bucket`, `blob`
                # have become stale, hence should be recreated.
                self._client = None
                self._bucket = None
                self._blob = None
            except NotFound as e:
                raise FileNotFoundError(self) from e

    def remove_file(self):
        try:
            self._rate_limit(self.blob().delete)
            self._blob = None
            return 1
        except NotFound:
            self._blob = None
            return 0

    def riterdir(self):
        prefix = self.blob_name + '/'
        k = len(prefix)
        for p in self.client.list_blobs(self.bucket, prefix=prefix):
            obj = self / p.name[k:]
            obj._blob = p
            yield obj

    def with_path(self, *paths: str):
        obj = self.__class__(*paths, bucket_name=self._bucket_name,
                             account_info=self._account_info)
        obj._client = self._client
        obj._bucket = self._bucket
        obj._blob = None
        return obj

    def write_bytes(self, data, *, overwrite=False):
        if self._path == '/':
            raise UnsupportedOperation("can not write to root as a blob", self)
        if isinstance(data, BufferedReader):
            data = data.read()
        nbytes = len(data)
        b = self.blob()

        # TODO: look into `retry`.

        if overwrite:
            self._rate_limit(b.upload_from_string, data, retry=None)
            # this will overwrite existing content.
        else:
            try:
                self._rate_limit(b.upload_from_string,
                                 data,
                                 if_generation_match=0)
            except PreconditionFailed:
                raise FileExistsError(self)
        return nbytes
