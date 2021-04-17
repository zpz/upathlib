import os
import shutil
from pathlib import Path

from zpz.dropbox import LocalFileStore


def test_local_file_store():
    root = '/tmp/test_local'
    shutil.rmtree(root, ignore_errors=True)
    os.mkdir(root)

    store = LocalFileStore()

    f = os.path.join(root, 'x.txt')
    assert not store.is_file(f)
    Path(f).write_text('test')
    assert store.is_file(f)
    store.rm(f)
    assert not store.is_file(f)

    f = os.path.join(root, 'sub/')
    assert not store.is_dir(f)
    os.mkdir(f)
    assert store.is_dir(f)
    store.rm_dir(f)
    assert not store.is_dir(f)

    f = os.path.join(root, 'x.txt')
    store.write_bytes(b'abc', f)
    assert store.read_bytes(f) == b'abc'

    f = os.path.join(root, 'b.txt')
    store.write_text('abc', f)
    assert store.read_text(f) == 'abc'

    f = os.path.join(root, 'sub/c.txt')
    store.write_text('def', f)

    z = store.ls(root, recursive=True)
    assert sorted(z) == [f'{root}/b.txt',
                         f'{root}/sub/',
                         f'{root}/sub/c.txt',
                         f'{root}/x.txt',
                         ]

    z = store.ls(root, recursive=False)
    assert sorted(z) == [f'{root}/b.txt',
                         f'{root}/sub/',
                         f'{root}/x.txt',
                         ]
