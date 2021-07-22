import pathlib
import upathlib.tests
from upathlib import LocalUpath

import pytest


def test_localupath_init():
    p = LocalUpath()
    assert p._path == str(pathlib.Path.cwd())
    p = LocalUpath('a', 'b', 'c', 'd')
    assert str(p.path) == str(pathlib.Path(
        pathlib.Path.cwd(), 'a', 'b', 'c', 'd'))


def test_all():
    p = LocalUpath('/tmp/upathlib_local_test')
    upathlib.tests.test_all(p)


@pytest.mark.asyncio
async def test_all_a():
    p = LocalUpath('/tmp/upathlib_local_test')
    await upathlib.tests.test_all_a(p)


def test_lock():
    p = LocalUpath('/tmp/upathlib_local_test')
    upathlib.tests.test_lock(p)


@pytest.mark.asyncio
async def test_a_lock():
    p = LocalUpath('/tmp/upathlib_local_test')
    await upathlib.tests.test_a_lock(p)
