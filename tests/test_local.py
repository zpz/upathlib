import multiprocessing as mp
import os
import pathlib
from time import sleep
from uuid import uuid4

import upathlib._tests

import pytest
from upathlib import LocalUpath


@pytest.fixture
def test_path():
    if os.name == 'posix':
        p = LocalUpath('/tmp/upathlib_local_test') / str(uuid4())
    else:
        p = LocalUpath(str(pathlib.Path.home() / 'tmp/upathlib_local_test')) / str(uuid4())
    try:
        p.rmrf()
        yield p
    finally:
        p.rmrf()


def test_localupath_init():
    p = LocalUpath()
    assert p._path == str(pathlib.Path.cwd())
    p = LocalUpath('a', 'b', 'c', 'd')
    assert str(p.path) == str(pathlib.Path(
        pathlib.Path.cwd(), 'a', 'b', 'c', 'd'))


def test_all(test_path):
    upathlib._tests.test_all(test_path)


def test_lock(test_path):
    upathlib._tests.test_lock(test_path)


def test_rename(test_path):
    p = test_path
    p.rmrf()

    (p / "a/a.txt").write_text("a")
    (p / "b/b.txt").write_text("b")
    (p / "c/d/e.txt").write_text("e")
    (p / "c/d.txt").write_text("d")

    assert (p / "a/a.txt").read_text() == "a"

    p.joinpath("a/a.txt").rename_file("b/a.txt")
    assert not (p / "a/a.txt").exists()
    assert (p / "a/b/a.txt").read_text() == "a"

    pp = (p / "c").rename_dir("a/c")

    assert (pp / "d/e.txt").read_text() == "e"
    assert (pp / "d.txt").read_text() == "d"
    assert not (p / "c").exists()


def test_pathlike(test_path):
    p = test_path
    p.write_text('abc')
    with open(p) as file:
        assert file.read() == 'abc'


def mp_rmrf(p):
    p.rmrf()
    (p / 'c.txt').write_text('c')
    sleep(1.1)


def test_mp(test_path):
    p = test_path
    p.rmrf()  # this creates a thread pool
    print('')
    for c in ('fork', 'spawn'):
        (p / 'a.txt').write_text('a')
        (p / 'b.txt').write_text('b')
        print('context:', c)
        ctx = mp.get_context(c)
        worker = ctx.Process(target=mp_rmrf, args=(p,))
        worker.start()
        print('worker started')
        p.rmrf()
        worker.join()
        print('worker finished')
        assert p.is_dir()


