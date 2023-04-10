from upathlib.serializer import (
    JsonSerializer,
    Lz4PickleSerializer,
    PickleSerializer,
    ZPickleSerializer,
    ZstdPickleSerializer,
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
