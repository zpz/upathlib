import pathlib
import pytest
from upathlib import LocalUpath


def test_localupath_init():
    p = LocalUpath()
    assert p.root == str(pathlib.Path.cwd())
    p = LocalUpath('a', 'b', 'c', 'd')
    assert p.root == '/a'
    assert str(p.path) == '/b/c/d'


def test_localupath():
    p = LocalUpath('/tmp/upathlib_local')
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

    assert p.root == '/tmp/upathlib_local'
    p.cd('a')
    assert p.root == '/tmp/upathlib_local/a'
    assert not p.exists()
    p.mkdir()
    assert p.exists()
    p.joinpath('x.data').write_bytes(b'x')
    p.cd('..')
    assert p.root == '/tmp/upathlib_local'
    assert p.joinpath('a', 'x.data').read_bytes() == b'x'


def test_copy():
    source = LocalUpath('/tmp/upath-test-source')
    source.mkdir(parents=True, exist_ok=True)
    source.clear()

    target = LocalUpath('/tmp/upath-test-target')
    target.mkdir(parents=True, exist_ok=True)
    target.clear()
    target.rmdir()

    local_file = source / 'testfile'
    local_file.write_text('abc', overwrite=True)

    target.copy_in(local_file)
    target.joinpath('samplefile').copy_in(local_file)

    assert sorted(target.iterdir()) == [
        target / 'samplefile', target / 'testfile'
    ]
