import pytest
from upathlib import LocalUpath
from upathlib._fake_cloud import FakeBlobUpath


def test_1():
    init_path = '/tmp/test'
    p = FakeBlobUpath('/tmp/test', bucket='bucket_a')
    if p.is_dir():
        p.clear()
        assert not list(p.iterdir())

    p.joinpath('abc.txt').write_text('abc')

    assert (p / 'abc.txt').read_text() == 'abc'

    with pytest.raises(FileExistsError):
        p.joinpath('abc.txt').write_text('abcd')

    p.joinpath('abc.txt').write_text('abcd', overwrite=True)
    assert (p / 'abc.txt').read_text() == 'abcd'

    with p.joinpath('abc.txt').lock() as f:
        assert f.read_text() == 'abcd'

    assert p._path == init_path
    p /= 'a'
    assert p._path == f'{init_path}/a'
    assert not p.is_file()
    assert not p.is_dir()
    assert not p.exists()
    p.mkdir()
    assert not p.exists()
    assert p.is_dir()
    p.joinpath('x.data').write_bytes(b'x')
    p /= '..'
    assert p._path == init_path
    assert p.joinpath('a', 'x.data').read_bytes() == b'x'


def test_copy():
    source = FakeBlobUpath('/tmp/upath-test-source', bucket='bucket_b')
    source.mkdir(exist_ok=True)
    source.clear()

    target = LocalUpath('/tmp/upath-test-target')
    target.mkdir(exist_ok=True)
    target.clear()

    source_file = source / 'testfile'
    source_file.write_text('abc', overwrite=True)

    target.copy_from(source_file)
    assert (target / 'testfile').read_text() == 'abc'

    target.joinpath('samplefile').copy_from(source_file)

    assert sorted(target.iterdir()) == [
        target / 'samplefile', target / 'testfile'
    ]
