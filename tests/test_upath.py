import contextlib
import pathlib
from upathlib import Upath


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
