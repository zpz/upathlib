from pathlib import Path
from upathlib import Upath, LocalUpath, resolve_path, GcsBlobUpath
from google.auth.exceptions import DefaultCredentialsError
import pytest


def test_resolve_path():
    p = resolve_path('/abc/de')
    assert isinstance(p, LocalUpath)
    p = resolve_path(Path('./ab'))
    assert isinstance(p, LocalUpath)
    p = resolve_path(LocalUpath('./a'))
    assert isinstance(p, LocalUpath)
    
    p = resolve_path('gs://mybucket/abc')
    assert isinstance(p, GcsBlobUpath)

    