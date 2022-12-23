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

The implementation for Azure was once in production use, but not anymore by myself, hence is unreliable.
There is a much older implementation for AWS, which was moved to "archive", because it was not updated to the current API.
