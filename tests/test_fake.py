import pytest
from upathlib._local import LocalUpath
from upathlib._fake import FakeBlobUpath


def test_1():
    init_path = '/tmp/test'
    p = FakeBlobUpath('/tmp/test', bucket='bucket_a')
    p.rmrf()

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
    assert not p.isfile()
    assert not p.isdir()
    assert not p.exists()
    p.joinpath('x.data').write_bytes(b'x')
    p /= '..'
    assert p._path == init_path
    assert p.joinpath('a', 'x.data').read_bytes() == b'x'


def test_copy():
    source = FakeBlobUpath('/tmp/upath-test-source', bucket='bucket_b')
    source.rmrf()

    target = LocalUpath('/tmp/upath-test-target')
    target.rmrf()

    source_file = source / 'testfile'
    source_file.write_text('abc', overwrite=True)

    target.copy_from(source_file)
    assert target.read_text() == 'abc'

    with pytest.raises(NotADirectoryError):
        # cant' write to `target/'samplefile'`
        # because `target` is a file.
        target.joinpath('samplefile').copy_from(source)

    target.rmrf()
    (target / 'samplefile').copy_from(source)
    assert target.ls() == [target / 'samplefile']
    assert target.joinpath('samplefile').ls() == [
        target/'samplefile'/'testfile']
    assert (target / 'samplefile' / 'testfile').read_text() == 'abc'