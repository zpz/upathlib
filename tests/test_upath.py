import contextlib
import pathlib
from upathlib._upath import Upath


class MyUpath(Upath):
    def exists(self):
        raise NotImplementedError

    def file_info(self):
        raise NotImplementedError

    def isdir(self):
        raise NotImplementedError

    def isfile(self):
        raise NotImplementedError

    def iterdir(self):
        raise NotImplementedError

    @contextlib.contextmanager
    def lock(self, wait=100):
        yield self

    def read_bytes(self):
        raise NotImplementedError

    def riterdir(self):
        raise NotImplementedError

    def rmdir(self):
        raise NotImplementedError

    def rmfile(self):
        raise NotImplementedError

    def write_bytes(self):
        raise NotImplementedError


def test_upath():
    p = MyUpath('abc/def/')
    assert p.path == pathlib.PurePosixPath('/abc/def')
    assert repr(p) == "MyUpath('/abc/def')"

    p = MyUpath('x/y/z')
    assert p.path == pathlib.PurePosixPath('/x/y/z')
    assert repr(p) == "MyUpath('/x/y/z')"


def test_upath_joinpath():
    p = MyUpath('abc/def/', 'x/y')
    pp = p / 'ab.txt'
    assert str(pp.path) == '/abc/def/x/y/ab.txt'

    pp = p.joinpath('../a/b.txt')
    assert pp == MyUpath('abc/def', 'x/a/b.txt')
    assert pp.name == 'b.txt'
    assert pp.suffix == '.txt'

    pp = p / '../../../../'
    assert str(pp.path) == '/'

    pp = p.joinpath('a', '.', 'b/c.data')
    assert str(pp.path) == '/abc/def/x/y/a/b/c.data'


def test_upath_cd():
    p = MyUpath('abc/def')
    assert p.path == pathlib.PurePosixPath('/abc/def')
    p /= 'xy/z'
    assert str(p.path) == '/abc/def/xy/z'
    assert p._path == str(p.path)
    p /= '..'
    assert p._path == '/abc/def/xy'
    p.joinpath('..')._path == '/abc/def'
    p.joinpath('..', '..', '..', '..')._path == '/'


def test_upath_compare():
    assert MyUpath('abc/def') / \
        'x/y/z' == MyUpath('/abc/def/x/y', 'z')
    assert MyUpath('abc/def') < MyUpath('abc/def', 'x')
    assert MyUpath('abc/def/x', 'y/z') > MyUpath('abc/def', 'x/y')
