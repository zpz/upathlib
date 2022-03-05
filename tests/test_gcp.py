from datetime import datetime
from unittest.mock import Mock, patch
import pathlib
import upathlib.tests
from upathlib.gcp import GcpBlobUpath, NotFound

import pytest


class Blob:
    def __init__(self, name, bucket):
        self.name = name
        self._bucket = bucket

    def delete(self):
        try:
            del self._bucket._blobs[self.name]
        except KeyError:
            raise NotFound(self.name)

    def exists(self):
        return self.name in self._bucket._blobs

    def upload_from_string(self, data, *, if_generation_match=None, **ignore):
        if if_generation_match == 0 and self.name in self._bucket._blobs:
            raise FileExistsError(self.name)
        self._bucket._blobs[self.name] = {
            'data': data,
            'time_created': datetime.now(),
            'updated': datetime.now(),
            'size': len(data),
            }

    def download_as_bytes(self):
        try:
            z = self._bucket._blobs[self.name]
            return z['data']
        except KeyError:
            raise NotFound(self.name)

    def download_to_filename(self, filename):
        upathlib.LocalUpath(filename).write_bytes(self.download_as_bytes())

    def upload_from_filename(self, filename):
        self.upload_from_string(upathlib.LocalUpath(filename).read_bytes())

    @property
    def time_created(self):
        return self._bucket._blobs[self.name]['time_created']

    @property
    def updated(self):
        return self._bucket._blobs[self.name]['updated']

    @property
    def size(self):
        return self._bucket._blobs[self.name]['size']

    @property
    def _properties(self):
        return {}


class Bucket:
    def __init__(self, name):
        self._name = name
        self._blobs = {}

    def blob(self, name):
        return Blob(name, self)

    def get_blob(self, name):
        if name in self._blobs:
            return self.blob(name)

    def copy_blob(self, blob, target_bucket, target_blob_name):
        target_bucket.blob(target_blob_name).upload_from_string(
                blob.download_as_bytes())


class Page:
    def __init__(self, blob_name):
        self.name = blob_name

    @property
    def prefixes(self):
        yield self.name


class BlobLists:
    def __init__(self, bucket, prefix, delimiter=None):
        self._bucket = bucket
        self._prefix = prefix
        self._delimiter = delimiter

    def __iter__(self):
        zz = [x for x in self._bucket._blobs
              if x.startswith(self._prefix)]
        if not self._delimiter:
            yield from (self._bucket.blob(z) for z in zz)
        else:
            k = len(self._prefix)
            yield from (
                    self._bucket.blob(z) for z in zz
                    if self._delimiter not in z[k:]
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

    def list_blobs(self, bucket, prefix, delimiter=None):
        return BlobLists(bucket, prefix, delimiter)



@pytest.fixture()
def gcp(mocker):
    mocker.patch('upathlib.gcp.service_account')
    mocker.patch('upathlib.gcp.storage.Client', Client)
    c = GcpBlobUpath(
            '/tmp/test',
            bucket_name='test',
            account_info={'project_id': 'abc'},
            )
    yield c


def test_all(gcp):
    upathlib.tests.test_all(gcp)


# def test_lock(gcp):
#     upathlib.tests.test_lock(gcp)
