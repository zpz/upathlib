from __future__ import annotations
# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.

import logging
import os
import random
import time
from contextlib import contextmanager
from datetime import datetime
from io import UnsupportedOperation

from azure.storage.blob import ContainerClient, BlobClient, BlobLeaseClient
# from azure.storage.blob.aio import (
# ContainerClient as aContainerClient,
# BlobClient as aBlobClient,
# BlobLeaseClient as aBlobLeaseClient,
# )
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError, HttpResponseError
from overrides import overrides

from ._upath import LockAcquisitionTimeoutError, FileInfo, Upath
from ._blob import BlobUpath
from ._local import LocalUpath

logging.getLogger('azure.storage').setLevel(logging.WARNING)
logging.getLogger('azure.core.pipeline.policies').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class AzureBlobUpath(BlobUpath):
    def __init__(self,
                 *paths: str,
                 account_name: str,
                 account_key: str,
                 sas_token: str,
                 container_name: str,
                 ):
        super().__init__(*paths)

        self._account_name = account_name
        self._account_key = account_key
        self._sas_token = sas_token
        self._account_url = f"https://{account_name}.blob.core.windows.net"
        self._container_name = container_name

        self._container_client: ContainerClient = None
        self._blob_client: BlobClient = None
        # self._a_container_client: Optional[aContainerClient] = None
        # self._a_blob_client: Optional[aBlobClient] = None
        self._lease: BlobLeaseClient = None
        self._lock_count: int = 0

    def __repr__(self) -> str:
        return "{}('{}', container_name='{}')".format(
            self.__class__.__name__, self._path, self._container_name
        )

    def __str__(self) -> str:
        return f"{self._container_name}://{self._path}"

    def __eq__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._container_name != self._container_name:
            return NotImplemented
        return self._path == other._path

    def __lt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._container_name != self._container_name:
            return NotImplemented
        return self._path < other._path

    def __le__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._container_name != self._container_name:
            return NotImplemented
        return self._path <= other._path

    def __gt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._container_name != self._container_name:
            return NotImplemented
        return self._path > other._path

    def __ge__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        if other._container_name != self._container_name:
            return NotImplemented
        return self._path >= other._path

    @property
    def container_name(self):
        return self._container_name

    def _copy_file_from(self, source):
        with self._provide_blob_client():
            with source._provide_blob_client():
                copy = self._blob_client.start_copy_from_url(
                    source._blob_client.url,
                    requires_sync=True,
                )
                assert copy['copy_status'] == 'success'

    @overrides
    def _copy_file(self, target: AzureBlobUpath):
        target._copy_file_from(self)

    @overrides
    def _export_file(self, target: Upath) -> None:
        if not isinstance(target, LocalUpath):
            return super()._export_file(target)
        with self._provide_blob_client():
            os.makedirs(str(target.parent), exist_ok=True)
            with open(str(target), 'wb') as f:
                data = self._blob_client.download_blob()
                data.readinto(f)

    @overrides
    def file_info(self):
        try:
            with self._provide_blob_client():
                info = self._blob_client.get_blob_properties()
                return FileInfo(
                    ctime=info.creation_time.timestamp(),
                    mtime=info.last_modified.timestamp(),
                    time_created=info.creation_time,
                    time_modified=info.last_modified,
                    size=info.size,
                    details=info,
                )
                # If an existing file is written to again using
                # `write_...(..., overwrite=True)`,
                # then its `ctime` will not change; only `mtime`
                # will be updated.
        except ResourceNotFoundError:
            return None

    @overrides
    def _import_file(self, source: Upath):
        if not isinstance(source, LocalUpath):
            return super()._import_file(source)
        with self._provide_blob_client():
            with open(str(source), 'rb') as data:
                self._blob_client.upload_blob(data)

    @overrides
    def is_file(self) -> bool:
        with self._provide_blob_client():
            return self._blob_client.exists()

    @overrides
    def iterdir(self):
        with self._provide_container_client():
            prefix = self.blob_name + '/'
            k = len(prefix)
            for p in self._container_client.walk_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    def _acquire_lease(self, timeout: int = None):
        if timeout is None:
            timeout = 3600
        t0 = time.perf_counter()
        while True:
            try:
                self.write_text(
                    datetime.utcnow().isoformat(), overwrite=True)
                try:
                    self._lease = self._blob_client.acquire_lease(
                        lease_duration=-1,
                        timeout=10)
                    return
                except ResourceNotFoundError:
                    continue  # go to the outer looper to write the file again
                except HttpResponseError as e:
                    if e.status_code == 409 and e.error_code == 'LeaseAlreadyPresent':  # pylint: disable=no-member
                        # Having a lease held by others. Continue to wait.
                        # This may happen when another client placed the lease
                        # on this blob right after we've created it, that is,
                        # another worker's won out in `acquire_lease`.
                        pass
                    else:
                        raise
            except HttpResponseError as e:
                if e.status_code == 412 and e.error_code == 'LeaseIdMissing':  # pylint: disable=no-member
                    # Blob exists and has a lease on it. Wait and try again.
                    pass
                else:
                    raise

            t1 = time.perf_counter()
            if t1 - t0 >= timeout:
                raise LockAcquisitionTimeoutError(self, t1 - t0)
            time.sleep(random.uniform(0.05, 1.0))

    @contextmanager
    @overrides
    def lock(self, *, timeout=None):
        '''
        References:
        https://docs.microsoft.com/en-us/azure/storage/blobs/concurrency-manage?tabs=dotnet
        '''
        with self._provide_blob_client():
            if self._lease is None:
                self._acquire_lease(timeout)
                self._lock_count = 1
            else:
                self._lock_count += 1
            try:
                yield
            finally:
                self._lock_count -= 1
                if self._lock_count <= 0:
                    self._lease.release()
                    self._lease = None
                    self._lock_count = 0

    # @asynccontextmanager
    # async def a_lock(self, *, timeout=None):
    #     loop = asyncio.get_running_loop()
    #     with self._provide_blob_client():
    #         # TODO: this context manager is sync

    #         if self._lease is None:
    #             ff = functools.partial(self._acquire_lease, timeout=timeout)
    #             await loop.run_in_executor(None, ff)
    #             self._lock_count = 1
    #         else:
    #             self._lock_count += 1
    #         try:
    #             yield
    #         finally:
    #             self._lock_count -= 1
    #             if self._lock_count <= 0:
    #                 await loop.run_in_executor(None, self._lease.release)
    #                 self._lease = None
    #                 self._lock_count = 0

    @ contextmanager
    def _provide_blob_client(self):
        if self._blob_client is None:
            bc = BlobClient(
                account_url=self._account_url,
                container_name=self._container_name,
                blob_name=self.blob_name,
                credential=self._sas_token or self._account_key,
            )
            self._blob_client = bc
            try:
                with bc:
                    yield
            finally:
                self._blob_client = None
        else:
            yield

    # TODO: how to optimize this part so that
    # new objects can reuse the ContainerClient?

    @ contextmanager
    def _provide_container_client(self):
        if self._container_client is None:
            cc = ContainerClient(
                account_url=self._account_url,
                container_name=self._container_name,
                credential=self._sas_token or self._account_key,
            )
            self._container_client = cc
            try:
                with cc:
                    yield
            finally:
                self._container_client = None
        else:
            yield

    @overrides
    def read_bytes(self) -> bytes:
        with self._provide_blob_client():
            try:
                return self._blob_client.download_blob().readall()
            except ResourceNotFoundError as e:
                raise FileNotFoundError(self) from e

    @overrides
    def remove_file(self) -> int:
        with self._provide_blob_client():
            try:

                self._blob_client.delete_blob(
                    delete_snapshots='include',
                    lease=self._lease)
                return 1
            except ResourceNotFoundError:
                return 0

    @overrides
    def riterdir(self):
        with self._provide_container_client():
            prefix = self.blob_name + '/'
            k = len(prefix)
            for p in self._container_client.list_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    @overrides
    def with_path(self, *paths):
        return self.__class__(
            *paths,
            account_name=self._account_name,
            account_key=self._account_key,
            sas_token=self._sas_token,
            container_name=self._container_name,
        )

    @overrides
    def write_bytes(self, data: bytes, *, overwrite=False) -> int:
        if self._path == '/':
            raise UnsupportedOperation(
                "can not write to root as a blob", self)

        with self._provide_blob_client():
            nbytes = len(data)
            try:
                self._blob_client.upload_blob(
                    data,
                    overwrite=overwrite,
                    lease=self._lease)
            except ResourceExistsError as e:
                raise FileExistsError(self) from e
            return nbytes
