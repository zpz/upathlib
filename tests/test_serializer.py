from concurrent.futures import ThreadPoolExecutor

from upathlib.serializer import (
    JsonSerializer,
    Lz4PickleSerializer,
    PickleSerializer,
    ZPickleSerializer,
    ZstdPickleSerializer,
    _MyLocal,
)

data = [12, 23.8, {'a': [9, 'xyz'], 'b': {'first': 3, 'second': 2.3}}, None]


def test_all():
    for serde in (
        JsonSerializer,
        PickleSerializer,
        ZPickleSerializer,
        ZstdPickleSerializer,
        Lz4PickleSerializer,
    ):
        y = serde.serialize(data)
        z = serde.deserialize(y)
        assert z == data


def test_mylocal():
    me = _MyLocal()
    assert len(me.compressor) == 0
    assert me.decompressor is None
    me.compressor[(1, 2)] = 3
    me.decompressor = 8

    def _check():
        assert len(me.compressor) == 0
        assert me.decompressor is None
        me.compressor[(3, 4)] = 5
        me.decompressor = 'a'
        return True

    with ThreadPoolExecutor() as pool:
        t = pool.submit(_check)
        assert t.result()

    assert me.compressor == {(1, 2): 3}
    assert me.decompressor == 8


def _check(data):
    y = ZstdPickleSerializer.serialize(data)
    z = ZstdPickleSerializer.deserialize(y)
    assert z == data
    return True


def test_zstd():
    assert _check(data)

    with ThreadPoolExecutor() as pool:
        t = pool.submit(_check, data=data)
        assert t.result()
