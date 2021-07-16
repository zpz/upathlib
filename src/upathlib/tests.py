import asyncio
import time
import pytest
from upathlib import Upath, LocalUpath

# User runs these tests with a `Upath` object of their subclass,
# in a path that is safe for testing.


def test_read_write_rm_navigate(p: Upath):
    init_path = p._path
    p.rmrf()

    p1 = p / 'abc.txt'
    assert not p1.exists()
    p1.write_text('abc')
    assert p1.isfile()
    assert not p1.isdir()
    assert p1.exists()
    assert p1.read_text() == 'abc'

    with pytest.raises(FileExistsError):
        p1.write_text('abcd')

    p1.write_text('abcd', overwrite=True)
    assert p1.read_text() == 'abcd'

    p /= 'a'
    assert p._path == f'{init_path}/a'
    assert not p.isfile()
    assert not p.isdir()
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
    assert fi1.size > fi2.size

    assert p3.isdir()
    assert p3.rmdir() == 1
    assert not p3.exists()
    assert not p2.exists()

    with pytest.raises(FileNotFoundError):
        p3.rmdir()

    assert p3.rmdir(missing_ok=True) == 0

    assert p.ls() == [p1]

    with pytest.raises(FileNotFoundError):
        p1.rmdir()

    assert p1.rmfile() == 1

    with pytest.raises(FileNotFoundError):
        p1.rmfile()
    assert p1.rmfile(missing_ok=True) == 0

    assert p.rmrf() == 0


async def test_a_read_write_rm_navigate(p: Upath):
    init_path = p._path
    await p.a_rmrf()

    p1 = p / 'abc.txt'
    assert not await p1.a_exists()
    await p1.a_write_text('abc')
    assert await p1.a_isfile()
    assert not await p1.a_isdir()
    assert await p1.a_exists()
    assert await p1.a_read_text() == 'abc'

    with pytest.raises(FileExistsError):
        await p1.a_write_text('abcd')

    await p1.a_write_text('abcd', overwrite=True)
    assert await p1.a_read_text() == 'abcd'

    p /= 'a'
    assert p._path == f'{init_path}/a'
    assert not await p.a_isfile()
    assert not await p.a_isdir()
    assert not await p.a_exists()

    await asyncio.sleep(0.3)
    # wait to ensure the next file as diff timestamp
    # from the first file.

    p2 = p.joinpath('x.data')
    await p2.a_write_bytes(b'x')
    p /= '..'
    assert p._path == init_path
    assert p2 == p.joinpath('a', 'x.data')
    assert await p2.a_read_bytes() == b'x'

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
    assert fi1.size > fi2.size

    assert await p3.a_isdir()
    assert await p3.a_rmdir() == 1
    assert not await p3.a_exists()
    assert not await p2.a_exists()

    with pytest.raises(FileNotFoundError):
        await p3.a_rmdir()

    assert await p3.a_rmdir(missing_ok=True) == 0

    assert await p.a_ls() == [p1]

    with pytest.raises(FileNotFoundError):
        await p1.a_rmdir()

    assert await p1.a_rmfile() == 1

    with pytest.raises(FileNotFoundError):
        await p1.a_rmfile()
    assert await p1.a_rmfile(missing_ok=True) == 0

    assert await p.a_rmrf() == 0


def test_copy(p: Upath):
    source = p
    source.rmrf()

    target = LocalUpath('/tmp/upath-test-target')
    target.rmrf()

    source_file = source / 'testfile'
    source_file.write_text('abc', overwrite=True)

    target.copy_from(source_file)
    assert target.read_text() == 'abc'

    with pytest.raises(NotADirectoryError):
        # cant' write to `target/'samplefile'`
        # because `target` is a file.
        target.joinpath('samplefile').copy_from(source)

    target.rmrf()
    (target / 'samplefile').copy_from(source)
    assert target.ls() == [target / 'samplefile']
    assert target.joinpath('samplefile').ls() == [
        target/'samplefile'/'testfile']
    assert (target / 'samplefile' / 'testfile').read_text() == 'abc'


async def test_a_copy(p: Upath):
    pass


def test_lock(p: Upath):
    pass


async def test_a_lock(p: Upath):
    pass


def test_all(p: Upath):
    test_read_write_rm_navigate(p)
    test_copy(p)
    test_lock(p)


async def test_all_a(p: Upath):
    await test_a_read_write_rm_navigate(p)
    await test_a_copy(p)
    await test_a_lock(p)
