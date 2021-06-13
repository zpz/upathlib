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
    p = MyUpath('abc/def/', root='/')
    assert p._root == '/'
    assert p.root == MyUpath(root=p._root)
    assert p.path == pathlib.PurePosixPath('/abc/def')
    assert repr(p) == "MyUpath('abc/def', root='/')"
    print('hash:', hash(p))

    p = MyUpath('x/y/z', root='/abc/def')
    assert p.path == pathlib.PurePosixPath('/x/y/z')
    assert p.root == MyUpath(root='/abc/def')
    assert repr(p) == "MyUpath('x/y/z', root='/abc/def')"
    print('hash:', hash(p))


def test_upath_joinpath():
    p = MyUpath('abc/def/', 'x/y', root='/')
    pp = p / 'ab.txt'
    assert str(pp.path) == '/abc/def/x/y/ab.txt'

    pp = p.joinpath('../a/b.txt')
    assert pp == MyUpath('abc/def', 'x/a/b.txt', root='/')
    assert pp.name == 'b.txt'
    assert pp.suffix == '.txt'

    pp = p / '../../../../'
    assert str(pp.path) == '/'

    pp = p.joinpath('a', '.', 'b/c.data')
    assert str(pp.path) == '/abc/def/x/y/a/b/c.data'


def test_upath_cd():
    p = MyUpath('abc/def', root='/')
    assert p.path == pathlib.PurePosixPath('/abc/def')
    p /= 'xy/z'
    assert str(p.path) == '/abc/def/xy/z'
    assert p._path == str(p.path)
    p /= '..'
    assert p._path == '/abc/def/xy'
    p.joinpath('..')._path == '/abc/def'
    p.joinpath('..', '..', '..', '..')._path == '/'


def test_upath_compare():
    assert MyUpath('abc/def', root='/') / \
        'x/y/z' == MyUpath('/abc/def/x/y', 'z', root='/')
    assert MyUpath('abc/def', root='/') < MyUpath('abc/def', 'x', root='/')
    assert MyUpath('abc/def/x', 'y/z',
                   root='/') > MyUpath('abc/def', 'x/y', root='/')
