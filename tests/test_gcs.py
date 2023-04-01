from datetime import datetime, timedelta
from io import BytesIO
from types import SimpleNamespace
from uuid import uuid4

import pytest
import upathlib._tests
from upathlib.gcs import GcsBlobUpath, exceptions

NotFound = exceptions.NotFound


class Blob:
    def __init__(self, name, bucket):
        self.name = name
        self._bucket = bucket

    def delete(self, client=None, if_generation_match=None):
        try:
            del self._bucket._blobs[self.name]
        except KeyError:
            raise NotFound(self.name)

    def exists(self, client=None):
        return self.name in self._bucket._blobs

    def reload(self, client=None):
        if not self.exists(client):
            raise NotFound(self.name)

    def _get_content_type(self, content_type, filename):
        return 'ok'

    def upload_from_file(self, data, *, if_generation_match=None, **ignore):
        if if_generation_match == 0 and self.name in self._bucket._blobs:
            raise FileExistsError(self.name)
        data = data.read()
        self._bucket._blobs[self.name] = {
            'data': data,
            'time_created': datetime.now(),
            'time_updated': datetime.now(),
            'size': len(data),
        }

    def download_to_file(self, file_obj, client=None):
        try:
            z = self._bucket._blobs[self.name]
            file_obj.write(z['data'])
        except KeyError:
            raise NotFound(self.name)

    @property
    def time_created(self):
        return self._bucket._blobs[self.name]['time_created']

    @property
    def updated(self):
        return self._bucket._blobs[self.name]['time_updated']

    @property
    def size(self):
        return self._bucket._blobs[self.name]['size']

    @property
    def _properties(self):
        return {}

    @property
    def generation(self):
        return 0


class Bucket:
    def __init__(self, name):
        self._name = name
        self._blobs = {}

    def blob(self, name):
        return Blob(name, self)

    def get_blob(self, name):
        if name in self._blobs:
            return self.blob(name)

    def copy_blob(
        self,
        blob,
        target_bucket,
        target_blob_name,
        client=None,
        if_generation_match=None,
    ):
        buffer = BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        target_bucket.blob(target_blob_name).upload_from_file(
            buffer, if_generation_match=if_generation_match
        )


class Page:
    def __init__(self, blob_name):
        self.name = blob_name

    @property
    def prefixes(self):
        yield self.name


class BlobLists:
    def __init__(self, bucket, prefix, delimiter=None, max_results=None):
        self._bucket = bucket
        self._prefix = prefix
        self._delimiter = delimiter
        self._max_results = max_results

    def __iter__(self):
        zz = [x for x in self._bucket._blobs if x.startswith(self._prefix)]
        if not self._delimiter:
            yield from (self._bucket.blob(z) for z in zz)
        else:
            k = len(self._prefix)
            yield from (
                self._bucket.blob(z) for z in zz if self._delimiter not in z[k:]
            )

    @property
    def prefixes(self):
        if self._delimiter:
            k = len(self._prefix)
            zz = []
            for x in self._bucket._blobs:
                if x.startswith(self._prefix):
                    n = x[k:]
                    if self._delimiter in n:
                        kk = n.find(self._delimiter)
                        zz.append(x[: (k + kk + 1)])
            zz = set(zz)
            return zz
        return []


class Client:
    def __init__(self, *args, **kwargs):
        pass

    def bucket(self, name):
        return Bucket(name)

    def list_blobs(
        self,
        bucket,
        prefix,
        delimiter=None,
        max_results=None,
        page_size=None,
        fields=None,
    ):
        return BlobLists(bucket, prefix, delimiter, max_results=max_results)


@pytest.fixture()
def gcp(mocker):
    # mocker.patch('upathlib.gcp.service_account')
    mocker.patch('upathlib.gcs.storage.Client', Client)
    mocker.patch('upathlib.gcs.GcsBlobUpath._PROJECT_ID', 'abc')
    mocker.patch(
        'upathlib.gcs.GcsBlobUpath._CREDENTIALS',
        SimpleNamespace(
            token='x', expiry=datetime.utcnow() + timedelta(days=1)  # noqa: S106
        ),  # noqa: S106
    )  # noqa: S106
    mocker.patch('upathlib.gcs.GcsBlobUpath._CLIENT', Client())
    c = GcsBlobUpath(
        '/tmp/test',
        bucket_name='test',
    ) / str(uuid4())
    print('c._bucket', c._bucket)
    c.rmrf()
    try:
        yield c
    finally:
        c.rmrf()


def test_all(gcp):
    print('gcp._bucket:', gcp._bucket)

    upathlib._tests.test_all(gcp)


# def test_lock(gcp):
#     upathlib._tests.test_lock(gcp)
