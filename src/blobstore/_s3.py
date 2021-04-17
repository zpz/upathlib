import logging
from pathlib import Path
import time

import boto3

# This module requires a directory `.aws/` containing credentials in the home directory,
# or environment variables `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY`.


logger = logging.getLogger(__name__)


def _get_client():
    return boto3.session.Session().client('s3')


def _has_key(s3_client, bucket: str, key: str) -> bool:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
    for obj in response.get('Contents', []):
        if obj['Key'] == key:
            return True
    return False


def _delete_key(s3_client, bucket: str, key: str) -> None:
    s3_client.delete_object(Bucket=bucket, Key=key)


def has_key(bucket: str, key: str) -> bool:
    return _has_key(_get_client(), bucket, key)


def delete_key(bucket: str, key: str) -> None:
    return _delete_key(_get_client(), bucket, key)


class Bucket:
    def __init__(self, bucket):
        for header in ('s3://', 's3n://'):
            if bucket.startswith(header):
                bucket = bucket[len(header):]
                break
        if '/' in bucket:
            bucket = bucket[: bucket.find('/')]
        self._bucket = boto3.resource('s3').Bucket(bucket)

    @property
    def name(self):
        return self._bucket.name

    def _remove_bucket_key(self, key):
        for header in ('s3://', 's3n://'):
            if key.startswith(header):
                assert key.startswith(header + self.name + '/')
                key = key[(len(header) + len(self.name) + 1):]
        return key

    def upload(self, local_file: str, s3_key: str) -> None:
        '''
        Upload a single file to S3.

        `local_file`: path to local file.
        `s3_key`: S3 'key'.

        Example: suppose current bucket is s3://my-org, with

        local_file: /home/zepu/work/data/xyz/memo.txt
        s3_key: mysurvey/memo
        --> remote file: s3://my-org/mysurvey/memo

        Existing file with the same name with be overwritten.
        '''
        local_file = Path(local_file)
        if not local_file.is_file():
            raise Exception('a file name is expected')
        data = open(local_file, 'rb')
        s3_key = self._remove_bucket_key(s3_key)
        self._bucket.put_object(Key=s3_key, Body=data)

    def upload_tree(self, local_path: str, s3_path: str,
                    pattern: str = '**/*') -> None:
        '''
        `local_path`: directory whose content will be uploaded.
            If `local_path` contains a trailing `/`, then no part of this path name
            becomes part of the remote name; otherwise, the final node in this path name
            becomes the leading segment of the remote name.
        `pattern`: 
            '*'    (everything directly under `local_path`),
            '**/*' (everything recursively under `local_path`),
            '*.py' (every Python module directly under `local_path`),
            '**/*.py' (every Python module recursively under `local_path`),
            etc.

        Example: suppose current bucket is s3://my-org, with

        local_path: /home/me/work/data/xyz, containing
            .../xyz/a.txt, 
            .../xyz/b.txt,
            ../xyz/zyx/aa.txt)
        s3_path: dataset1
        s3_name: '**/*'
        --> remote files: 
            s3://my-org/dataset1/xyz/a.txt
            s3://my-org/dataset1/xyz/b.txt
            s3://my-org/dataset1/xyz/zyx/aa.txt

        local_path: /home/me/work/data/xyz/ (note the trailing '/')
        --> remote files: 
            s3://my-org/dataset1/a.txt
            s3://my-org/dataset1/b.txt
            s3://my-org/dataset1/zyx/aa.txt
        '''
        with_root = not local_path.endswith('/')
        local_path = Path(local_path)
        if not local_path.is_dir():
            raise Exception('a directory name is expected')
        nodes = [v for v in local_path.glob(pattern) if v.is_file()]
        s3_path = self._remove_bucket_key(s3_path)
        for node in nodes:
            key = node.relative_to(local_path)
            if with_root:
                key = local_path.name / key
            key = s3_path / key
            self.upload(node, str(key))

    def download(self, s3_key: str, local_file: str = None) -> None:
        s3_key = self._remove_bucket_key(s3_key)
        if local_file is None:
            local_file = str(Path(s3_key).name)
        self._bucket.download_file(s3_key, local_file)

    def download_tree(self, s3_path: str, local_path: str = None) -> None:
        s3_path = self._remove_bucket_key(s3_path)
        raise NotImplementedError

    def ls(self, key, recursive: bool = False):
        # List object names directly or recursively named like `key*`.
        # If `key` is `abc/def/`,
        # then `abc/def/123/45` will return as `123/45`
        #
        # If `key` is `abc/def`,
        # then `abc/defgh/45` will return as `defgh/45`;
        # `abc/def/gh` will return as `/gh`.
        #
        # So if you know `key` is a `directory`, then it's a good idea to
        # include the trailing `/` in `key`.

        key = self._remove_bucket_key(key)

        z = self._bucket.objects.filter(Prefix=key)

        if key.endswith('/'):
            key_len = len(key)
        else:
            key_len = key.rfind('/') + 1

        if recursive:
            return (v.key[key_len:] for v in z)
            # this is a generator, b/c there can be many, many elements
        else:
            keys = set()
            for v in z:
                vv = v.key[key_len:]
                idx = vv.find('/')
                if idx >= 0:
                    vv = vv[: idx]
                keys.add(vv)
            return sorted(list(keys))

    def has(self, key: str) -> bool:
        key = self._remove_bucket_key(key)

        if not hasattr(self, '_s3'):
            self._s3 = _get_client()
        return _has_key(self._s3, self._bucket.name, key)

    def delete(self, key: str) -> None:
        key = self._remove_bucket_key(key)

        if not hasattr(self, '_s3'):
            self._s3 = _get_client()
        _delete_key(self._s3, self._bucket.name, key)

    def delete_tree(self, s3_path: str) -> int:
        s3_path = self._remove_bucket_key(s3_path)

        n = 0
        while True:
            nn = self._delete_tree(s3_path)
            if nn == 0:
                break
            n = max(n, nn)
            time.sleep(0.5)
        return n

    def _delete_tree(self, s3_path: str) -> int:
        '''
        Return the number of objects deleted.

        After this operation, the 'folder' `s3_path` is also gone.

        TODO: this is not the fastest way to do it.
        '''
        assert s3_path.endswith('/')
        n = 0
        for k in self.ls(s3_path, recursive=True):
            kk = s3_path + k
            self.delete(kk)
            n += 1
        return n


def reduce_boto_logging():
    import boto3.s3.transfer
    assert boto3.s3.transfer  # silence pyflakes
    for name in logging.Logger.manager.loggerDict.keys():
        if name.startswith('boto') or name.startswith('urllib3') or name.startswith('s3transfer'):
            logging.getLogger(name).setLevel(logging.ERROR)
