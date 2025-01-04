upathlib
========

------ UPDATE ------

After 0.9.8 (November 2024), development of this package continues in the repo 
`cloudly <https://github.com/zpz/cloudly>`_
as part of the ``cloudly`` package.

``upathlib`` has been used in production for several years.
Although I feel the API is pretty stable, I still recommend you to switch to using it from ``cloudly``.

------ END OF UPDATE ------

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

3.10 or newer.
