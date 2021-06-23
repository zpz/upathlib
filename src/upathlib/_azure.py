import time
import threading
from contextlib import contextmanager
from datetime import datetime
from dateutil.parser import parse
from typing import Optional


from azure.storage.blob import ContainerClient, BlobClient, BlobLeaseClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

from ._upath import BlobUpath, LockAcquisitionTimeoutError


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
        self._lease_id = None
        self._lock_count = 0
        self._t_renew_lease = None
        self._t_renew_lease_stopped = False

    def __repr__(self) -> str:
        return "{}('{}', container_name='{}'".format(
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

    def _blob_exists(self):
        with self._provide_blob_client():
            return self._blob_client.exists()

    def iterdir(self):
        with self._provide_container_client():
            prefix = self._path.lstrip('/') + '/'
            k = len(prefix)
            for p in self._container_client.walk_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    # TODO:
    # `a_lock` needs reimplementation, as the raw thread
    # will not work with async.

    @contextmanager
    def lock(self, *, wait=60):
        with self._provide_blob_client():
            if self._lease_id is None:
                t0 = time.perf_counter()
                while True:
                    try:
                        t1 = time.perf_counter()
                        if t1 - t0 > wait:
                            raise LockAcquisitionTimeoutError(
                                str(self), t1 - t0)
                        self._lease_id = self._blob_client.acquire_lease(
                            lease_duration=60,
                            timeout=t1 - t0).id
                        self._t_renew_lease = threading.Thread(
                            target=self._renew_lease)
                        self._t_renew_lease.start()
                        break
                    except ResourceNotFoundError:
                        try:
                            self.write_text(
                                datetime.utcnow().isoformat(), overwrite=False)
                        except ResourceExistsError:
                            # Somehow another worker has just created this blob.
                            # Continue to wait.
                            continue
            self._lock_count += 1
            try:
                yield
            finally:
                self._lock_count -= 1
                if self._lock_count <= 0:
                    self._t_renew_lease_stopped = True
                    self._t_renew_lease.join()
                    self._t_renew_lease_stopped = False
                    self._t_renew_lease = None
                    self.rm()
                    BlobLeaseClient(self._blob_client,
                                    lease_id=self._lease_id).release()
                    # TODO:
                    # is the order of the two statements above correct?
                    self._lease_id = None
                    self._lock_count = 0

    @ contextmanager
    def _provide_blob_client(self):
        if self._blob_client is None:
            bc = BlobClient(
                account_url=self._account_url,
                container_name=self._container_name,
                blob_name=self._path.lstrip('/'),
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

    def _recursive_iterdir(self):
        with self._provide_container_client():
            prefix = self._path.lstrip('/') + '/'
            k = len(prefix)
            for p in self._container_client.list_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    def _renew_lease(self):
        t0 = time.perf_counter()
        while True:
            time.sleep(0.012)
            if self._t_renew_lease_stopped:
                return
            if time.perf_counter() - t0 >= 57:
                # Renew ahead of the lease duration 60 seconds.
                BlobLeaseClient(self._blob_client,
                                lease_id=self._lease_id).renew()
                t0 = time.perf_counter()

    def rm(self, missing_ok=False):
        with self._provide_blob_client():
            if not self.is_file():
                if missing_ok:
                    return 0
                if self.is_dir():
                    raise IsADirectoryError(self)
                raise FileNotFoundError(self)
            self._blob_client.delete_blob(delete_snapshots='include')
            return 1

    def stat(self):
        with self._provide_blob_client():
            info = self._blob_client.get_blob_properties()
            return {
                'created_at': parse(str(info.creation_time)),
                'modified_at': parse(str(info.last_modified)),
                'last_accessed_at': parse(str(info.last_accessed_on)),
                'size_bytes': info.size,
            }
            # TODO: refer to the local stat data structure.

    def write_bytes(self, data, *, overwrite=False):
        with self._provide_blob_client():
            super().write_bytes(data, overwrite=overwrite)
            nbytes = len(data)
            try:
                self._blob_client.upload_blob(
                    data,
                    overwrite=overwrite,
                    lease=self._lease_id)
            except ResourceExistsError as e:
                raise FileExistsError(self) from e
            return nbytes
