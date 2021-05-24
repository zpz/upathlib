import pytest
from upathlib import LocalUPath


def test_localupath():
    p = LocalUPath('/tmp/upathlib_local')
    p.clear()

    assert not p.ls_r()

    with pytest.raises(FileNotFoundError):
        p('abc.txt').write_text('abc')

    p('abc.txt').write_text('abc', parents=True)
    assert p('abc.txt').read_text() == 'abc'

    assert p.root == '/tmp/upathlib_local'
    p.cd('a')
    assert p.root == '/tmp/upathlib_local/a'
    assert not p.exists()
    p.mkdir()
    assert p.exists()
    p('x.data').write_bytes(b'x')
    p.cd('..')
    assert p.root == '/tmp/upathlib_local'
    assert p('a', 'x.data').read_bytes() == b'x'
