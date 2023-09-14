upathlib
========

The package ``upathlib``
defines a unified API for cloud blob store (aka "object store") as well as local file systems.

End user should look to the class ``Upath`` for documentation on the API.
Local file system and Google Cloud Storage are implemented by subclasses
``LocalUpath`` and ``GcsBlobUpath``, respectively.

To install, do one of the following::


    $ pip3 install upathlib
    $ pip3 install upathlib[gcs]


Read the `documentation <https://upathlib.readthedocs.io/en/latest/>`_.

Status
------

The style of the API is largely stable. The implementations for local file system and for Google Cloud Storage are production ready.


Python version
--------------

Development and testing were conducted in Python 3.8 until version 0.7.3.
Starting with 0.7.4, development and testing happen in Python 3.10.
Code continues to NOT intentionally use features beyond Python 3.8.
I think 3.10 may be required sometime in 2024.
