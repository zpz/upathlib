import os.path
import pathlib
import pytest
from upathlib import Upath, LocalUpath


def test_upath():
    p = Upath('abc/def/')
    assert p.root == '/abc/def'
    assert p._home == '/abc/def'
    assert p.home() == p
    assert p.path == pathlib.PurePosixPath('/')
    assert p.fullpath == pathlib.PurePosixPath('/abc/def/')
    assert repr(p) == "Upath('/abc/def', '')"
    assert str(p) == '/abc/def'
    print('hash:', hash(p))

    p = Upath('/abc/def', 'x/y/z')
    assert p.root == '/abc/def'
    assert p.path == pathlib.PurePosixPath('/x/y/z')
    assert p.fullpath == pathlib.PurePosixPath('/abc/def/x/y/z')
    assert p.home() == Upath('/abc/def')
    assert repr(p) == "Upath('/abc/def', 'x/y/z')"
    assert str(p) == '/abc/def/x/y/z'
    print('hash:', hash(p))


def test_upath_joinpath():
    p = Upath('abc/def/', 'x/y')
    pp = p / 'ab.txt'
    assert str(pp.path) == '/x/y/ab.txt'

    pp = p.joinpath('../a/b.txt')
    assert pp == Upath('abc/def', '/x/a/b.txt')

    pp = p / '../../../../'
    assert str(pp.path) == '/'

    pp = p.joinpath('a', '.', 'b/c.data')
    assert str(pp.path) == '/x/y/a/b/c.data'


def test_upath_cd():
    p = Upath('abc/def')
    assert p.root == '/abc/def'
    p.cd('xy/z')
    assert p.root == '/abc/def/xy/z'
    p.cd('..')
    assert p.root == '/abc/def/xy'
    p.cd('..').root == '/abc/def'
    p.cd('..').cd('..').cd('..').cd('..')
    assert p.root == '/'


def test_upath_compare():
    assert Upath('abc/def') / 'x/y/z' == Upath('/abc/def/x/y', 'z')
    assert Upath('abc/def') < Upath('abc/def', 'x')
    assert Upath('abc/def/x', 'y/z') > Upath('abc/def', 'x/y')


def test_localupath_init():
    p = LocalUpath()
    assert p.root == os.path.expanduser('~')
    p = LocalUpath('a', 'b', 'c', 'd')
    assert p.root == '/a'
    assert str(p.path) == '/b/c/d'


def test_localupath():
    p = LocalUpath('/tmp/upathlib_local')
    p.clear()

    assert not p.ls_r()

    with pytest.raises(FileNotFoundError):
        p.joinpath('abc.txt').write_text('abc')

    p.joinpath('abc.txt').write_text('abc', parents=True)
    assert (p / 'abc.txt').read_text() == 'abc'

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
