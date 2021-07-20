import asyncio
import logging
import os
import time
import threading
# from contextlib import contextmanager, asynccontextmanager
from contextlib import contextmanager
from datetime import datetime
from io import UnsupportedOperation
from typing import Optional, Union

from azure.storage.blob import ContainerClient, BlobClient, BlobLeaseClient  # type: ignore
# from azure.storage.blob.aio import (
# ContainerClient as aContainerClient,
# BlobClient as aBlobClient,
# BlobLeaseClient as aBlobLeaseClient,
# )
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError, HttpResponseError  # type: ignore

from ._upath import LockAcquisitionTimeoutError, FileInfo, Upath
from ._blob import BlobUpath
from ._local import LocalUpath

logging.getLogger('azure.storage').setLevel(logging.WARNING)
logging.getLogger('azure.core.pipeline.policies').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# TODO: async lock

class AzureBlobUpath(BlobUpath):
    def __init__(self,
                 *parts: str,
                 account_name: str,
                 account_key: str,
                 sas_token: str,
                 container_name: str,
                 ):
        super().__init__(
            *parts,
            account_name=account_name,
            account_key=account_key,
            sas_token=sas_token,
            container_name=container_name
        )

        self._account_name = account_name
        self._account_key = account_key
        self._sas_token = sas_token
        self._account_url = f"https://{account_name}.blob.core.windows.net"
        self._container_name = container_name

        self._container_client: Optional[ContainerClient] = None
        self._blob_client: Optional[BlobClient] = None
        # self._a_container_client: Optional[aContainerClient] = None
        # self._a_blob_client: Optional[aBlobClient] = None
        self._lease_id: Optional[str] = None
        self._lock_count: int = 0
        self._t_renew_lease: Optional[Union[threading.Thread,
                                            asyncio.Task]] = None
        self._t_renew_lease_stopped: bool = False

    def __repr__(self) -> str:
        return "{}('{}', container_name='{}')".format(
            self.__class__.__name__, self._path, self._container_name
        )

    def __str__(self) -> str:
        return f"{self._container_name}://{self._path}"

    def __eq__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._container_name != self._container_name):
            return NotImplemented
        return self._path == other._path

    def __lt__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._container_name != self._container_name):
            return NotImplemented
        return self._path < other._path

    def __le__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._container_name != self._container_name):
            return NotImplemented
        return self._path <= other._path

    def __gt__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._container_name != self._container_name):
            return NotImplemented
        return self._path > other._path

    def __ge__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._container_name != self._container_name):
            return NotImplemented
        return self._path >= other._path

    def _copy_file_from(self, source):
        with self._provide_blob_client():
            with source._provide_blob_client():
                copy = self._blob_client.start_copy_from_url(
                    source._blob_client.url,
                    requires_sync=True,
                )
                assert copy['copy_status'] == 'success'

    def _copy_file(self, target):
        target._copy_file_from(self)

    def _export_file(self, target: Upath):
        if not isinstance(target, LocalUpath):
            return super()._export_file(target)
        with self._provide_blob_client():
            os.makedirs(str(target.parent), exist_ok=True)
            with open(str(target), 'wb') as f:
                data = self._blob_client.download_blob()  # type: ignore
                data.readinto(f)

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

    def _import_file(self, source: Upath):
        if not isinstance(source, LocalUpath):
            return super()._import_file(source)
        with self._provide_blob_client():
            with open(str(source), 'rb') as data:
                self._blob_client.upload_blob(data)  # type: ignore

    def is_file(self):
        with self._provide_blob_client():
            return self._blob_client.exists()

    def iterdir(self):
        with self._provide_container_client():
            prefix = self._blob_name + '/'
            k = len(prefix)
            for p in self._container_client.walk_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    @contextmanager
    def lock(self, *, wait=60):
        with self._provide_blob_client():
            if self._lease_id is None:
                t0 = time.perf_counter()
                while True:
                    try:
                        self.write_text(
                            datetime.utcnow().isoformat(), overwrite=True)
                        try:
                            self._lease_id = self._blob_client.acquire_lease(
                                lease_duration=60,
                                timeout=1).id
                            break
                        except ResourceNotFoundError:
                            continue  # go to the outer looper to write the file again
                        except HttpResponseError as e:
                            if e.status_code == 409 and e.error_code == 'LeaseAlreadyPresent':
                                # Having a lease held by others. Continue to wait.
                                pass
                            else:
                                raise
                    except HttpResponseError as e:
                        if e.status_code == 412 and e.error_code == 'LeaseIdMissing':
                            # Blob exists and has a lease on it. Wait and try again.
                            pass
                        else:
                            raise

                    t1 = time.perf_counter()
                    if t1 - t0 >= wait:
                        raise LockAcquisitionTimeoutError(self, t1 - t0)
                    time.sleep(0.011)

                self._t_renew_lease = threading.Thread(
                    target=self._renew_lease)
                self._t_renew_lease_stopped = False
                self._t_renew_lease.start()

            self._lock_count += 1
            try:
                yield
            finally:
                self._lock_count -= 1
                if self._lock_count <= 0:
                    self._t_renew_lease_stopped = True
                    self._t_renew_lease.join()
                    self._t_renew_lease = None
                    self.remove_file()
                    # still holding the lease; this should succeed.
                    self._lease_id = None
                    self._lock_count = 0

    @ contextmanager
    def _provide_blob_client(self):
        if self._blob_client is None:
            bc = BlobClient(
                account_url=self._account_url,
                container_name=self._container_name,
                blob_name=self._blob_name,
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

    def read_bytes(self):
        with self._provide_blob_client():
            try:
                return self._blob_client.download_blob().readall()
            except ResourceNotFoundError as e:
                raise FileNotFoundError(self) from e

    def remove_file(self):
        with self._provide_blob_client():
            try:
                self._blob_client.delete_blob(
                    delete_snapshots='include',
                    lease=self._lease_id)
                return 1
            except ResourceNotFoundError:
                return 0

    def _renew_lease(self):
        t0 = time.perf_counter()
        while True:
            time.sleep(0.002)
            if self._t_renew_lease_stopped:
                self._t_renew_lease_stopped = False
                return
            if time.perf_counter() - t0 >= 13:
                # Renew ahead of the lease duration of 60 seconds.
                BlobLeaseClient(self._blob_client,
                                lease_id=self._lease_id).renew()
                t0 = time.perf_counter()

    def riterdir(self):
        with self._provide_container_client():
            prefix = self._blob_name + '/'
            k = len(prefix)
            for p in self._container_client.list_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    def write_bytes(self, data, *, overwrite=False):
        if self._path == '/':
            raise UnsupportedOperation(
                "can not write to root as a blob", self)

        with self._provide_blob_client():
            nbytes = len(data)
            try:
                self._blob_client.upload_blob(
                    data,
                    overwrite=overwrite,
                    lease=self._lease_id)
            except ResourceExistsError as e:
                raise FileExistsError(self) from e
            return nbytes

    # @ asynccontextmanager
    # async def a_lock(self, *, wait=60):
    #     async with self._a_provide_blob_client():
    #         if self._lease_id is None:
    #             loop = asyncio.get_running_loop()
    #             t0 = loop.time()
    #             while True:
    #                 try:
    #                     await self.a_write_text(
    #                         datetime.utcnow().isoformat(), overwrite=True)
    #                     try:
    #                         self._lease_id = (
    #                             await self._a_blob_client.acquire_lease(
    #                                 lease_duration=60,
    #                                 timeout=1)).id
    #                         break
    #                     except ResourceNotFoundError:
    #                         continue  # go to the outer looper to write the file again
    #                     except HttpResponseError as e:
    #                         if e.status_code == 409 and e.error_code == 'LeaseAlreadyPresent':
    #                             # Having a lease held by others. Continue to wait.
    #                             pass
    #                         else:
    #                             raise
    #                 except HttpResponseError as e:
    #                     if e.status_code == 412 and e.error_code == 'LeaseIdMissing':
    #                         # Blob exists and has a lease on it. Wait and try again.
    #                         pass
    #                     else:
    #                         raise

    #                 t1 = loop.time()
    #                 if t1 - t0 >= wait:
    #                     raise LockAcquisitionTimeoutError(self, t1 - t0)
    #                 await asyncio.sleep(0.011)

    #             self._t_renew_lease_stopped = False
    #             self._t_renew_lease = asyncio.create_task(
    #                 self._a_renew_lease)

    #         self._lock_count += 1
    #         try:
    #             yield
    #         finally:
    #             self._lock_count -= 1
    #             if self._lock_count <= 0:
    #                 self._t_renew_lease_stopped = True
    #                 await self._t_renew_lease
    #                 self._t_renew_lease = None
    #                 await self.a_remove_file()
    #                 # still holding the lease; this should succeed.
    #                 self._lease_id = None
    #                 self._lock_count = 0

    # @ asynccontextmanager
    # async def _a_provide_blob_client(self):
    #     if self._a_blob_client is None:
    #         bc = aBlobClient(
    #             account_url=self._account_url,
    #             container_name=self._container_name,
    #             blob_name=self._blob_name,
    #             credential=self._sas_token or self._account_key,
    #         )
    #         self._a_blob_client = bc
    #         try:
    #             async with bc:
    #                 yield
    #         finally:
    #             self._a_blob_client = None
    #     else:
    #         yield

    # @ asynccontextmanager
    # async def _a_provide_container_client(self):
    #     if self._a_container_client is None:
    #         cc = aContainerClient(
    #             account_url=self._account_url,
    #             container_name=self._container_name,
    #             credential=self._sas_token or self._account_key,
    #         )
    #         self._a_container_client = cc
    #         try:
    #             async with cc:
    #                 yield
    #         finally:
    #             self._a_container_client = None
    #     else:
    #         yield

    # async def _a_renew_lease(self):
    #     loop = asyncio.get_running_loop()
    #     t0 = loop.time()
    #     while True:
    #         await asyncio.sleep(0.002)
    #         if self._t_renew_lease_stopped:
    #             self._t_renew_lease_stopped = False
    #             return
    #         if loop.time() - t0 >= 13:
    #             # Renew ahead of the lease duration of 60 seconds.
    #             await aBlobLeaseClient(
    #                 self._blob_client, lease_id=self._lease_id).renew()
    #             t0 = loop.time()

    # async def a_riterdir(self):
    #     async with self._a_provide_container_client():
    #         prefix = self._blob_name + '/'
    #         k = len(prefix)
    #         async for p in self._a_container_client.list_blobs(
    #                 name_starts_with=prefix):
    #             yield self / p.name[k:]
