# type: ignore

import asyncio
import concurrent.futures
import functools
import pathlib
import time
import pytest
from upathlib import Upath, LocalUpath, LockAcquisitionTimeoutError

# User runs these tests with a `Upath` object of their subclass,
# in a path that is safe for testing.


def test_basic(p: Upath):
    pp = p / '/abc/def/'
    assert pp.path == pathlib.PurePosixPath('/abc/def')
    print(repr(pp))

    pp = pp / 'x/y/z'
    assert pp.path == pathlib.PurePosixPath('/abc/def/x/y/z')
    print(repr(pp))

    pp /= 'xy/z'
    assert str(pp.path) == '/abc/def/x/y/z/xy/z'
    assert pp._path == str(pp.path)
    pp /= '..'
    assert pp._path == '/abc/def/x/y/z/xy'
    pp.joinpath('..')._path == '/abc/def/x/y/z'
    pp.joinpath('..', '..', '..', '..', '..')._path == '/'


def test_joinpath(path: Upath):
    try:
        pp = path.joinpath('/abc/def/', 'x/y') / 'ab.txt'
        assert str(pp.path) == '/abc/def/x/y/ab.txt'

        pp = pp.joinpath('../a/b.txt')
        assert pp == path / '/abc/def' / 'x/y/a/b.txt'
        assert pp.name == 'b.txt'
        assert pp.suffix == '.txt'

        p = pp

        pp = pp / '../../../../'
        assert str(pp.path) == '/abc/def'

        pp = p.joinpath('a', '.', 'b/c.data')
        assert str(pp.path) == '/abc/def/x/y/a/b.txt/a/b/c.data'
    except:
        print('')
        print('repr:  ', repr(pp))
        print('str:   ', str(pp))
        print('path:  ', pp.path)
        print('_path: ', pp._path)
        raise


def test_compare(p: Upath):
    assert p.joinpath('abc/def') / 'x/y/z' == p / 'abc/def/x/y' / 'z'
    assert p / 'abc/def' < p.joinpath('abc/def', 'x')
    assert p.joinpath('abc/def/x', 'y/z') > p.joinpath('abc/def', 'x/y')


def test_read_write_rm_navigate(p: Upath):
    init_path = p._path
    p.rmrf()

    p1 = p / 'abc.txt'
    assert not p1.exists()
    p1.write_text('abc')
    assert p1.is_file()
    assert not p1.is_dir()
    assert p1.exists()
    assert p1.read_text() == 'abc'

    with pytest.raises(FileExistsError):
        p1.write_text('abcd')

    p1.write_json({'data': 'abcd'}, overwrite=True)
    assert p1.read_json() == {'data': 'abcd'}

    p /= 'a'
    assert p._path == f'{init_path}/a'
    assert not p.is_file()
    assert not p.is_dir()
    assert not p.exists()

    time.sleep(0.3)
    # wait to ensure the next file as diff timestamp
    # from the first file.

    p2 = p.joinpath('x.data')
    p2.write_bytes(b'x')
    p /= '..'
    assert p._path == init_path
    assert p2 == p.joinpath('a', 'x.data')
    assert p2.read_bytes() == b'x'

    p3 = p / 'a'

    assert p.ls() == [p3, p1]
    assert sorted(p.riterdir()) == [p2, p1]
    assert p3.file_info() is None
    fi1 = p1.file_info()
    fi2 = p2.file_info()
    print('')
    print('p1:', fi1)
    print('p2:', fi2)
    print('')
    assert fi1.mtime < fi2.mtime
    print('file 1 size:', fi1.size)
    print('file 2 size:', fi2.size)
    assert fi1.size > fi2.size

    assert p3.is_dir()
    assert p3.remove_dir() == 1
    assert not p3.exists()
    assert not p2.exists()

    assert p3.remove_dir() == 0

    assert p.ls() == [p1]

    assert p1.remove_dir() == 0

    assert p1.remove_file() == 1

    assert p1.remove_file() == 0

    assert p.rmrf() == 0


async def test_a_read_write_rm_navigate(p: Upath):
    init_path = p._path
    await p.a_rmrf()

    p1 = p / 'abc.txt'
    assert not await p1.a_exists()
    await p1.a_write_text('abc')
    assert await p1.a_is_file()
    assert not await p1.a_is_dir()
    assert await p1.a_exists()
    assert await p1.a_read_text() == 'abc'

    with pytest.raises(FileExistsError):
        await p1.a_write_text('abcd')

    await p1.a_write_json({'data': 'abcd'}, overwrite=True)
    assert await p1.a_read_json() == {'data': 'abcd'}

    p /= 'a'
    assert p._path == f'{init_path}/a'
    assert not await p.a_is_file()
    assert not await p.a_is_dir()
    assert not await p.a_exists()

    await asyncio.sleep(0.3)
    # wait to ensure the next file as diff timestamp
    # from the first file.

    p2 = p.joinpath('x.data')
    await p2.a_write_orjson_z(['x', 'y', 13])
    p /= '..'
    assert p._path == init_path
    assert p2 == p.joinpath('a', 'x.data')
    assert await p2.a_read_orjson_z() == ['x', 'y', 13]

    p3 = p / 'a'

    assert await p.a_ls() == [p3, p1]

    pp = [p async for p in p.a_riterdir()]
    assert sorted(pp) == [p2, p1]
    assert await p3.a_file_info() is None
    fi1 = await p1.a_file_info()
    fi2 = await p2.a_file_info()
    print('')
    print('p1:', fi1)
    print('p2:', fi2)
    print('')
    assert fi1.mtime < fi2.mtime
    print('file 1 size:', fi1.size)
    print('file 2 size:', fi2.size)
    assert fi1.size < fi2.size

    assert await p3.a_is_dir()
    assert await p3.a_remove_dir() == 1
    assert not await p3.a_exists()
    assert not await p2.a_exists()

    assert await p3.a_remove_dir() == 0

    assert await p.a_ls() == [p1]

    assert await p1.a_remove_dir() == 0

    assert await p1.a_remove_file() == 1

    assert await p1.a_remove_file() == 0

    assert await p.a_rmrf() == 0


def test_import_export(p: Upath):
    source = p
    source.rmrf()

    target = LocalUpath('/tmp/upath-test-target')
    target.rmrf()

    source_file = source / 'testfile'
    source_file.write_text('abc', overwrite=True)

    target.import_file(source_file)
    assert target.read_text() == 'abc'

    with pytest.raises(NotADirectoryError):
        # cant' write to `target/'samplefile'`
        # because `target` is a file.
        target.joinpath('samplefile').import_dir(source)

    target.rmrf()
    p2 = target.joinpath('samplefile')
    p2.import_dir(source)
    p3 = p2 / source_file.name
    assert target.ls() == [p2]
    assert p2.ls() == [p3]
    assert p3.read_text() == 'abc'

    p1 = source / 'a' / 'b' / 'c'
    assert p2.export_dir(p1) == 1
    p4 = p1 / source_file.name
    assert p4.read_text() == 'abc'

    assert p2.export_dir(source / 'a' / 'b') == 1
    assert (source / 'a' / 'b' / source_file.name).read_text() == 'abc'


async def test_a_import_export(p: Upath):
    source = p
    await source.a_rmrf()

    target = LocalUpath('/tmp/upath-test-target')
    await target.a_rmrf()

    source_file = source / 'testfile'
    await source_file.a_write_text('abc', overwrite=True)

    await target.a_import_file(source_file)
    assert await target.a_read_text() == 'abc'

    with pytest.raises(NotADirectoryError):
        # cant' write to `target/'samplefile'`
        # because `target` is a file.
        await target.joinpath('samplefile').a_import_dir(source)

    await target.a_rmrf()
    p2 = target.joinpath('samplefile')
    await p2.a_import_dir(source)
    p3 = p2 / source_file.name
    assert await target.a_ls() == [p2]
    assert await p2.a_ls() == [p3]
    assert await p3.a_read_text() == 'abc'

    p1 = source / 'a' / 'b' / 'c'
    assert await p2.a_export_dir(p1) == 1
    p4 = p1 / source_file.name
    assert await p4.a_read_text() == 'abc'

    assert await p2.a_export_dir(source / 'a' / 'b') == 1
    assert await (source / 'a' / 'b' /
                  source_file.name).a_read_text() == 'abc'


def test_rename(p: Upath):
    p.rmrf()

    (p / 'a/a.txt').write_text('a')
    (p / 'b/b.txt').write_text('b')
    (p / 'c/d/e.txt').write_text('e')
    (p / 'c/d.txt').write_text('d')

    p.joinpath('a/a.txt').rename_file('b/a.txt')
    assert not (p / 'a/a.txt').exists()
    assert (p / 'a/b/a.txt').read_text() == 'a'

    pp = (p / 'c').rename_dir('a/c')

    # print(p)
    # for x in p.riterdir():
    #     print(x)

    assert (pp / 'd/e.txt').read_text() == 'e'
    assert (pp / 'd.txt').read_text() == 'd'
    assert not (p / 'c').exists()


async def test_a_rename(p: Upath):
    await p.a_rmrf()

    await (p / 'a/a.txt').a_write_text('a')
    await (p / 'b/b.txt').a_write_text('b')
    await (p / 'c/d/e.txt').a_write_text('e')
    await (p / 'c/d.txt').a_write_text('d')

    await p.joinpath('a/a.txt').a_rename_file('b/a.txt')
    assert not await (p / 'a/a.txt').a_exists()
    assert await (p / 'a/b/a.txt').a_read_text() == 'a'

    pp = await (p / 'c').a_rename_dir('a/c')

    # print(p)
    # async for x in p.a_riterdir():
    #     print(x)

    assert await (pp / 'd/e.txt').a_read_text() == 'e'
    assert await (pp / 'd.txt').a_read_text() == 'd'
    assert not await (p / 'c').a_exists()


def _access_in_mp(root: Upath, path: str, timeout):
    p = root / path
    t0 = time.perf_counter()
    try:
        with p.lock(timeout=timeout):
            return time.perf_counter() - t0
    except LockAcquisitionTimeoutError:
        return t0 - time.perf_counter()


def test_lock(p: Upath, timeout=None, wait=3):
    p.rmrf()
    pp = p / 'testlock'
    with pp.lock(timeout=timeout):
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            t = pool.submit(_access_in_mp, p / '/', pp._path, wait)
            z = t.result()
            print('mp returned after', z, 'seconds')
            assert z <= -wait


async def test_a_lock(p: Upath, timeout=None, wait=3):
    await p.a_rmrf()
    pp = p / 'testlock'
    async with pp.a_lock(timeout=timeout):
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            ff = functools.partial(_access_in_mp, p / '/', pp._path, wait)
            z = await asyncio.get_running_loop().run_in_executor(pool, ff)
            print('mp returned after', z, 'seconds')
            assert z <= -wait


def test_all(p: Upath):
    test_basic(p)
    test_joinpath(p)
    test_compare(p)

    test_read_write_rm_navigate(p)
    test_import_export(p)
    test_rename(p)


async def test_all_a(p: Upath):
    await test_a_read_write_rm_navigate(p)
    await test_a_import_export(p)
    await test_a_rename(p)
