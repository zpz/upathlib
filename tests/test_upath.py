import contextlib
import pathlib
import pytest
from upathlib import Upath, LocalUpath


class MyUpath(Upath):
    def exists(self):
        raise NotImplementedError

    def is_dir(self):
        raise NotImplementedError

    def is_file(self):
        raise NotImplementedError

    def iterdir(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def lock(self, wait=100):
        yield self

    def mkdir(self):
        raise NotImplementedError

    def read_bytes(self):
        raise NotImplementedError

    def rm(self):
        raise NotImplementedError

    def rmdir(self):
        raise NotImplementedError

    def stat(self):
        raise NotImplementedError

    def write_bytes(self, data, overwrite=False):
        raise NotImplementedError


def test_upath():
    p = MyUpath('abc/def/')
    assert p.root == '/abc/def'
    assert p._home == '/abc/def'
    assert p.home() == p
    assert p.path == pathlib.PurePosixPath('/')
    assert p.fullpath == pathlib.PurePosixPath('/abc/def/')
    assert repr(p) == "MyUpath('/abc/def', '')"
    assert str(p) == '/abc/def'
    print('hash:', hash(p))

    p = MyUpath('/abc/def', 'x/y/z')
    assert p.root == '/abc/def'
    assert p.path == pathlib.PurePosixPath('/x/y/z')
    assert p.fullpath == pathlib.PurePosixPath('/abc/def/x/y/z')
    assert p.home() == MyUpath('/abc/def')
    assert repr(p) == "MyUpath('/abc/def', 'x/y/z')"
    assert str(p) == '/abc/def/x/y/z'
    print('hash:', hash(p))


def test_upath_joinpath():
    p = MyUpath('abc/def/', 'x/y')
    pp = p / 'ab.txt'
    assert str(pp.path) == '/x/y/ab.txt'

    pp = p.joinpath('../a/b.txt')
    assert pp == MyUpath('abc/def', '/x/a/b.txt')

    pp = p / '../../../../'
    assert str(pp.path) == '/'

    pp = p.joinpath('a', '.', 'b/c.data')
    assert str(pp.path) == '/x/y/a/b/c.data'


def test_upath_cd():
    p = MyUpath('abc/def')
    assert p.root == '/abc/def'
    p.cd('xy/z')
    assert p.root == '/abc/def/xy/z'
    p.cd('..')
    assert p.root == '/abc/def/xy'
    p.cd('..').root == '/abc/def'
    p.cd('..').cd('..').cd('..').cd('..')
    assert p.root == '/'


def test_upath_compare():
    assert MyUpath('abc/def') / 'x/y/z' != MyUpath('/abc/def/x/y', 'z')
    assert MyUpath('abc/def') < MyUpath('abc/def', 'x')
    assert MyUpath('abc/def/x', 'y/z') > MyUpath('abc/def', 'x/y')


def test_localupath_init():
    p = LocalUpath()
    assert p.root == str(pathlib.Path.cwd())
    p = LocalUpath('a', 'b', 'c', 'd')
    assert p.root == '/a'
    assert str(p.path) == '/b/c/d'


def test_localupath():
    p = LocalUpath('/tmp/upathlib_local')
    p.clear(missing_ok=True)

    assert not list(p.iterdir(missing_ok=True))

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
