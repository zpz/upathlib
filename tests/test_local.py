import pathlib
import upathlib.tests
from upathlib import LocalUpath


def test_localupath_init():
    p = LocalUpath()
    assert p._path == str(pathlib.Path.cwd())
    p = LocalUpath('a', 'b', 'c', 'd')
    assert str(p.path) == str(pathlib.Path(
        pathlib.Path.cwd(), 'a', 'b', 'c', 'd'))


def test_all():
    p = LocalUpath('/tmp/upathlib_local_test')
    upathlib.tests.test_all(p)


def test_lock():
    p = LocalUpath('/tmp/upathlib_local_test')
    upathlib.tests.test_lock(p)
