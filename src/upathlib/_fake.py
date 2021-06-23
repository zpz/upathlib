import contextlib
from ._upath import BlobUpath


class ResourceNotFoundError(Exception):
    pass


class FakeBlobStore:
    '''A in-memory blobstore for illustration purposes'''

    def __init__(self):
        self._data = {
            'bucket_a': {},
            'bucket_b': {},
        }

    def write_bytes(self, bucket: str, name: str, data: bytes):
        print('write bytes', bucket, name, data)
        self._data[bucket][name] = data
        print('_data:', self._data)

    def read_bytes(self, bucket: str, name: str):
        z = self._data[bucket]
        try:
            return z[name]
        except KeyError:
            raise ResourceNotFoundError(name)

    def list_blobs(self, bucket: str, prefix: str):
        for k in self._data[bucket].keys():
            if k.startswith(prefix):
                yield k

    def delete_blob(self, bucket: str, name: str):
        z = self._data[bucket]
        try:
            del z[name]
        except KeyError:
            raise ResourceNotFoundError(name)

    def exists(self, bucket: str, name: str):
        z = self._data[bucket]
        return name in z


_store = FakeBlobStore()


class FakeBlobUpath(BlobUpath):
    '''This Upath implementation for the FakeBlobstore
    can be used for testing basic functionalities.

    This also showcases the essential methods that
    a concrete subclass of BlobUpath needs to implement.'''

    def __init__(self, *parts: str, bucket: str):
        super().__init__(*parts, bucket=bucket)
        self._bucket = bucket

    def _blob_exists(self):
        return _store.exists(self._bucket, self._path)

    @contextlib.contextmanager
    def lock(self, *, wait=60):
        # place holder
        yield self

    def read_bytes(self):
        try:
            return _store.read_bytes(self._bucket, self._path)
        except ResourceNotFoundError as e:
            raise FileNotFoundError(self) from e

    def _recursive_iterdir(self):
        p = self._path
        if not p.endswith('/'):
            p += '/'
        return _store.list_blobs(self._bucket, p)

    def rm(self, missing_ok=False):
        if not self.is_file():
            if missing_ok:
                return 0
            if self.is_dir():
                raise IsADirectoryError(self)
            raise FileNotFoundError(self)

        _store.delete_blob(self._bucket, self._path)
        return 1

    def stat(self):
        # place holder
        return {}

    def write_bytes(self, data, *, overwrite=False):
        super().write_bytes(data, overwrite=overwrite)
        _store.write_bytes(self._bucket, self._path, data)
