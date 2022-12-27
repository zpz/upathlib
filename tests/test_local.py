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


def test_rename():
    p = LocalUpath('/tmp/upathlib_local_test')
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
