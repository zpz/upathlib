from pathlib import Path
from upathlib import Upath, LocalUpath
from google.auth.exceptions import DefaultCredentialsError
import pytest


def test_resolve_path():
    p = Upath.resolve_path('/abc/de')
    assert isinstance(p, LocalUpath)
    p = Upath.resolve_path(Path('./ab'))
    assert isinstance(p, LocalUpath)
    p = Upath.resolve_path(LocalUpath('./a'))
    assert isinstance(p, LocalUpath)
    
    with pytest.raises(DefaultCredentialsError):
        p = Upath.resolve_path('gs://mybucket/abc')

    