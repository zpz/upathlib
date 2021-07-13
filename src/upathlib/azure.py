import asyncio
import logging
import time
import threading
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime
from io import UnsupportedOperation
from typing import Optional, Union

from azure.storage.blob import ContainerClient, BlobClient, BlobLeaseClient
from azure.storage.blob.aio import (
    ContainerClient as aContainerClient,
    BlobClient as aBlobClient,
    BlobLeaseClient as aBlobLeaseClient,
)
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError, HttpResponseError

from ._upath import LockAcquisitionTimeoutError, FileInfo
from ._blob import BlobUpath

logging.getLogger('azure.storage').setLevel(logging.WARNING)
logging.getLogger('azure.core.pipeline.policies').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class AzureBlobCredential:
    def __init__(self, *,
                 account_name: str,
                 account_key: str,
                 sas_token: str,
                 container_name: str):
        self._account_name = account_name
        self._account_key = account_key
        self._account_url = f"https://{account_name}.blob.core.windows.net"
        self._sas_token = sas_token
        self.container_name = container_name
        self._container_client = None
        self._a_container_client = None

    @contextmanager
    def blob_client(self, blob_name: str):
        with self.container_client() as cc:
            with cc.get_blob_client(blob_name) as bc:
                yield bc

    @contextmanager
    def container_client(self):
        if self._container_client is None:
            cc = ContainerClient(
                account_url=self._account_url,
                container_name=self.container_name,
                credential=self._sas_token or self._account_key,
            )
            self._container_client = cc
            try:
                with cc:
                    yield cc
            finally:
                self._container_client = None
        else:
            yield self._container_client

    @ asynccontextmanager
    async def a_blob_client(self, blob_name: str):
        async with self.a_container_client() as cc:
            async with cc.get_blob_client(blob_name) as bc:
                yield bc

    @ asynccontextmanager
    async def a_container_client(self):
        if self._a_container_client is None:
            cc = aContainerClient(
                account_url=self.account_url,
                container_name=self.container_name,
                credential=self._sas_token or self._account_key,
            )
            self._a_container_client = cc
            try:
                async with cc:
                    yield cc
            finally:
                self._a_container_client = None
        else:
            yield self._a_container_client


class AzureBlobUpath(BlobUpath):
    def __init__(self,
                 *parts: str,
                 credential: AzureBlobCredential,
                 ):
        super().__init__(*parts, credential=credential)
        self._credential = credential
        self._container_name = credential.container_name

        self._container_client: Optional[ContainerClient] = None
        self._blob_client: Optional[BlobClient] = None
        self._a_container_client: Optional[aContainerClient] = None
        self._a_blob_client: Optional[aBlobClient] = None
        self._lease_id: str = None
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

    def isfile(self):
        with self._provide_blob_client():
            return self._blob_client.exists()

    def iterdir(self):
        with self._provide_container_client():
            prefix = self._path.lstrip('/') + '/'
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
                    self.rmfile()
                    # still holding the lease; this should succeed.
                    self._lease_id = None
                    self._lock_count = 0

    @ contextmanager
    def _provide_blob_client(self):
        if self._blob_client is None:
            with self._credential.blob_client(self._path.lstrip('/')) as bc:
                self._blob_client = bc
                try:
                    yield
                finally:
                    self._blob_client = None
        else:
            yield

    @ contextmanager
    def _provide_container_client(self):
        if self._container_client is None:
            with self._credential.container_client() as cc:
                self._container_client = cc
                try:
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
            prefix = self._path.lstrip('/') + '/'
            k = len(prefix)
            for p in self._container_client.list_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    def rmfile(self, *, missing_ok=False):
        with self._provide_blob_client():
            try:
                self._blob_client.delete_blob(
                    delete_snapshots='include',
                    lease=self._lease_id)
                logger.info('deleting %s', self.path)
                # log this after successful deletion.
                return 1
            except ResourceNotFoundError as e:
                if missing_ok:
                    return 0
                raise FileNotFoundError(self) from e

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

    async def a_file_info(self):
        try:
            async with self._a_provide_blob_client():
                info = await self._a_blob_client.get_blob_properties()
                return FileInfo(
                    ctime=info.creation_time.timestamp(),
                    mtime=info.last_modified.timestamp(),
                    time_created=info.creation_time,
                    time_modified=info.last_modified,
                    size=info.size,
                    details=info,
                )
        except ResourceNotFoundError as e:
            raise FileNotFoundError(self) from e

    async def a_isfile(self):
        async with self._a_provide_blob_client():
            return await self._a_blob_client.exists()

    async def a_iterdir(self):
        async with self._a_provide_container_client():
            prefix = self._path.lstrip('/') + '/'
            k = len(prefix)
            async for p in self._a_container_client.walk_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    @ asynccontextmanager
    async def a_lock(self, *, wait=60):
        async with self._a_provide_blob_client():
            if self._lease_id is None:
                loop = asyncio.get_running_loop()
                t0 = loop.time()
                while True:
                    try:
                        await self.a_write_text(
                            datetime.utcnow().isoformat(), overwrite=True)
                        try:
                            self._lease_id = (
                                await self._a_blob_client.acquire_lease(
                                    lease_duration=60,
                                    timeout=1)).id
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

                    t1 = loop.time()
                    if t1 - t0 >= wait:
                        raise LockAcquisitionTimeoutError(self, t1 - t0)
                    await asyncio.sleep(0.011)

                self._t_renew_lease_stopped = False
                self._t_renew_lease = asyncio.create_task(
                    self._a_renew_lease)

            self._lock_count += 1
            try:
                yield
            finally:
                self._lock_count -= 1
                if self._lock_count <= 0:
                    self._t_renew_lease_stopped = True
                    await self._t_renew_lease
                    self._t_renew_lease = None
                    await self.a_rmfile(missing_ok=True)
                    # still holding the lease; this should succeed.
                    self._lease_id = None
                    self._lock_count = 0

    @ asynccontextmanager
    async def _a_provide_blob_client(self):
        if self._a_blob_client is None:
            async with self._credential.a_blob_client(self._path.lstrip('/')) as bc:
                self._a_blob_client = bc
                try:
                    yield
                finally:
                    self._a_blob_client = None
        else:
            yield

    @ asynccontextmanager
    async def _a_provide_container_client(self):
        if self._a_container_client is None:
            async with self._credential.a_container_client() as cc:
                self._a_container_client = cc
                try:
                    yield
                finally:
                    self._a_container_client = None
        else:
            yield

    async def a_read_bytes(self):
        async with self._a_provide_blob_client():
            try:
                return await (await self._a_blob_client.download_blob()).readall()
            except ResourceNotFoundError as e:
                raise FileNotFoundError(self) from e

    async def _a_renew_lease(self):
        loop = asyncio.get_running_loop()
        t0 = loop.time()
        while True:
            await asyncio.sleep(0.002)
            if self._t_renew_lease_stopped:
                self._t_renew_lease_stopped = False
                return
            if loop.time() - t0 >= 13:
                # Renew ahead of the lease duration of 60 seconds.
                await aBlobLeaseClient(
                    self._blob_client, lease_id=self._lease_id).renew()
                t0 = loop.time()

    async def a_riterdir(self):
        async with self._a_provide_container_client():
            prefix = self._path.lstrip('/') + '/'
            k = len(prefix)
            async for p in self._a_container_client.list_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    async def a_rmfile(self, *, missing_ok=False):
        async with self._a_provide_blob_client():
            try:
                await self._a_blob_client.delete_blob(
                    delete_snapshots='include',
                    lease=self._lease_id)
                logger.info('deleting %s', self.path)
                # log this after successful deletion.
                return 1
            except ResourceNotFoundError as e:
                if missing_ok:
                    return 0
                raise FileNotFoundError(self) from e

    async def a_write_bytes(self, data, *, overwrite=False):
        if self._path == '/':
            raise UnsupportedOperation(
                "can not write to root as a blob", self)

        async with self._a_provide_blob_client():
            nbytes = len(data)
            try:
                await self._a_blob_client.upload_blob(
                    data,
                    overwrite=overwrite,
                    lease=self._lease_id)
            except ResourceExistsError as e:
                raise FileExistsError(self) from e
            return nbytes
