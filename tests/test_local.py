import pathlib
import pytest
from upathlib._local import LocalUpath


def test_localupath_init():
    p = LocalUpath()
    assert p._path == str(pathlib.Path.cwd())
    p = LocalUpath('a', 'b', 'c', 'd')
    assert str(p.path) == str(pathlib.Path(
        pathlib.Path.cwd(), 'a', 'b', 'c', 'd'))


def test_localupath():
    p = LocalUpath('/tmp/upathlib_local')
    if p.isdir():
        p.rmdir()
        assert not list(p.iterdir())

    p.joinpath('abc.txt').write_text('abc')
    assert (p / 'abc.txt').read_text() == 'abc'

    with pytest.raises(FileExistsError):
        p.joinpath('abc.txt').write_text('abcd')

    p.joinpath('abc.txt').write_text('abcd', overwrite=True)
    assert (p / 'abc.txt').read_text() == 'abcd'

    with p.joinpath('abc.txt.lock').lock():
        assert p.joinpath('abc.txt').read_text() == 'abcd'

    assert p._path == '/tmp/upathlib_local'
    p /= 'a'
    assert p._path == '/tmp/upathlib_local/a'
    assert not p.exists()
    p.joinpath('x.data').write_bytes(b'x')
    p /= '..'
    assert p._path == '/tmp/upathlib_local'
    assert p.joinpath('a', 'x.data').read_bytes() == b'x'


def test_copy():
    source = LocalUpath('/tmp/upath-test-source')
    source.rmrf()

    local_file = source / 'testfile'
    local_file.write_text('abc', overwrite=True)

    target = LocalUpath('/tmp/upath-test-target')
    target.rmrf()

    target.joinpath('test').copy_from(local_file)
    assert target.joinpath('test').read_text() == 'abc'

    # Now `target` is a dir.
    target.copy_from(source)

    assert target.ls() == [
        target / 'test',
        target / 'upath-test-source',
    ]
