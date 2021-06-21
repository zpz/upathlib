import time
from contextlib import contextmanager
from typing import Optional

from dateutil.parser import parse

from azure.storage.blob import ContainerClient, BlobClient, BlobLeaseClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

from ._upath import BlobUpath


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

    def _blob_exists(self):
        return self._blob_client.exists()

    @contextmanager
    def lock(self, *, wait=60):
        t0 = time.perf_counter()
        with self._provide_blob_client():
            while True:
                if self._lease_id is not None:
                    self._lock_count += 1
                else:
                    pass
                break
            try:
                yield self
            finally:
                self._lock_count -= 1
                if self._lock_count <= 0:
                    self.rm()
                    BlobLeaseClient(self._blob_client,
                                    lease_id=self._lease_id).release()

    @contextmanager
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

    @contextmanager
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

    def recursive_iterdir(self):
        with self._provide_container_client():
            prefix = self._path.lstrip('/') + '/'
            k = len(prefix)
            for p in self._container_client.list_blobs(
                    name_starts_with=prefix):
                yield self / p.name[k:]

    def rm(self, missing_ok=False):
        with self._provide_blob_client():
            super().rm(missing_ok=missing_ok)
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
            self._blob_client.upload_blob(data, overwrite=overwrite)
            return nbytes
