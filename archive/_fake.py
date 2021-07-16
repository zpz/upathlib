import contextlib
from ._blob import BlobUpath


class ResourceNotFoundError(Exception):
    pass


class ResourceExistsError(Exception):
    pass


class FakeBlobStore:
    '''A in-memory blobstore for illustration purposes'''

    def __init__(self):
        self._data = {
            'bucket_a': {},
            'bucket_b': {},
        }

    def write_bytes(self, bucket: str, name: str, data: bytes, overwrite: bool = False):
        if name in self._data[bucket] and not overwrite:
            raise ResourceExistsError
        self._data[bucket][name] = data
        return len(data)

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

    def file_info(self):
        # place holder
        return {}

    def isfile(self):
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

    def riterdir(self):
        p = self._path
        if not p.endswith('/'):
            p += '/'
        for pp in _store.list_blobs(self._bucket, p):
            yield self / pp[len(p):]

    def rmfile(self, *, missing_ok=False):
        try:
            _store.delete_blob(self._bucket, self._path)
            return 1
        except ResourceNotFoundError as e:
            if missing_ok:
                return 0
            raise FileNotFoundError(self) from e

    def write_bytes(self, data, *, overwrite=False):
        try:
            _store.write_bytes(self._bucket, self._path,
                               data, overwrite=overwrite)
        except ResourceExistsError as e:
            raise FileExistsError(self) from e
