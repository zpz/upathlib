from io import BufferedReader
from google.oauth2 import service_account
from google.cloud import storage

from ._upath import BlobUpath


class GcpBlobUpath(BlobUpath):
    def __init__(self, *parts: str, bucket_name: str, account_info: dict):
        super().__init__(*parts, bucket_name=bucket_name, account_info=account_info)
        gcp_cred = service_account.Credentials.from_service_account_info(
            account_info)
        self._client = storage.Client(
            project=account_info['project_id'],
            credentials=gcp_cred,
        )
        self._bucket = self._client.get_bucket(bucket_name)

    def _blob_exists(self) -> bool:
        return self._bucket.blob(self._path.lstrip('/')).exists()

    def read_bytes(self):
        super().read_bytes()
        b = self._bucket.get_blob(self._path.lstrip('/'))
        if b is None:
            raise FileNotFoundError(self)
        return b.download_as_bytes()

    def _recursive_iterdir(self):
        prefix = self._path.lstrip('/') + '/'
        k = len(prefix)
        for p in self._client.list_blobs(self._bucket, prefix=prefix):
            yield self / p.name[k:]

    def rm(self, missing_ok=False):
        super().rm(missing_ok=missing_ok)
        b = self._bucket.blob(self._path.lstrip('/'))
        if not b.exists():
            if missing_ok:
                return
            raise FileNotFoundError(self)
        b.delete()

    def stat(self):
        # place holder
        return {}

    def write_bytes(self, data, *, overwrite=False):
        super().write_bytes(data, overwrite=overwrite)
        b = self._bucket.blob(self._path.lstrip('/'))
        if b.exists():
            if not overwrite:
                raise FileExistsError(self)
            b.delete()
        if isinstance(data, BufferedReader):
            data = data.read()
        b.upload_from_string(data)
