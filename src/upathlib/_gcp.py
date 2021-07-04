import contextlib
import logging
from io import BufferedReader, UnsupportedOperation
from google.oauth2 import service_account
from google.cloud import storage

from ._blob import BlobUpath

logger = logging.getLogger(__name__)


class GcpBlobUpath(BlobUpath):
    def __init__(self, *parts: str, bucket_name: str, account_info: dict):
        super().__init__(*parts,
                         bucket_name=bucket_name,
                         account_info=account_info)
        gcp_cred = service_account.Credentials.from_service_account_info(
            account_info)
        self._client = storage.Client(
            project=account_info['project_id'],
            credentials=gcp_cred,
        )
        self._bucket_name = bucket_name
        self._bucket = self._client.get_bucket(bucket_name)

    def __repr__(self) -> str:
        return "{}('{}', bucket_name='{}'".format(
            self.__class__.__name__, self._path, self._bucket_name
        )

    def __str__(self) -> str:
        return f"{self._bucket_name}://{self._path}"

    def __eq__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._bucket_name != self._bucket_name):
            return NotImplemented
        return self._path == other._path

    def __lt__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._bucket_name != self._bucket_name):
            return NotImplemented
        return self._path < other._path

    def __le__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._bucket_name != self._bucket_name):
            return NotImplemented
        return self._path <= other._path

    def __gt__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._bucket_name != self._bucket_name):
            return NotImplemented
        return self._path > other._path

    def __ge__(self, other) -> bool:
        if (other.__class__ is not self.__class__):
            return NotImplemented
        if (other._bucket_name != self._bucket_name):
            return NotImplemented
        return self._path >= other._path

    @property
    def _blob(self):
        return self._bucket.get_blob(self._path.lstrip('/'))
        # If the blob does not exist, this is `None`.

    def file_info(self):
        raise NotImplementedError

    def isfile(self) -> bool:
        # return self._bucket.blob(self._path.lstrip('/')).exists()
        return self._blob is not None

    @contextlib.contextmanager
    def lock(self, *, wait=60):
        raise NotImplementedError

    def read_bytes(self):
        b = self._blob
        if b is None:
            raise FileNotFoundError(self)
        return b.download_as_bytes()

    def riterdir(self):
        prefix = self._path.lstrip('/') + '/'
        k = len(prefix)
        for p in self._client.list_blobs(self._bucket, prefix=prefix):
            yield self / p.name[k:]

    def rmfile(self, *, missing_ok=False):
        b = self._blob
        if b is None:
            if missing_ok:
                return 0
            raise FileNotFoundError(self)

        logger.info('deleting %s', self.path)
        b.delete()
        return 1

    def write_bytes(self, data, *, overwrite=False):
        if self._path == '/':
            raise UnsupportedOperation("can not write to root as a blob", self)

        nbytes = len(data)

        b = self._blob
        if b is not None and not overwrite:
            raise FileExistsError(self)
        if isinstance(data, BufferedReader):
            data = data.read()
        b.upload_from_string(data)  # this will overwrite existing content.
        return nbytes
