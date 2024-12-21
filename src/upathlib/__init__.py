__version__ = '0.9.9'

from cloudly.upathlib import *

try:
    from cloudly.gcp.storage import GcsBlobUpath
except ImportError:
    pass
