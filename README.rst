
.. image:: https://travis-ci.org/MacHu-GWU/s3splitmerge-project.svg?branch=master
    :target: https://travis-ci.org/MacHu-GWU/s3splitmerge-project?branch=master

.. image:: https://codecov.io/gh/MacHu-GWU/s3splitmerge-project/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/s3splitmerge-project

.. image:: https://img.shields.io/pypi/v/s3splitmerge.svg
    :target: https://pypi.python.org/pypi/s3splitmerge

.. image:: https://img.shields.io/pypi/l/s3splitmerge.svg
    :target: https://pypi.python.org/pypi/s3splitmerge

.. image:: https://img.shields.io/pypi/pyversions/s3splitmerge.svg
    :target: https://pypi.python.org/pypi/s3splitmerge

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/s3splitmerge-project

------


.. image:: https://img.shields.io/badge/Link-Document-blue.svg
    :target: http://s3splitmerge.my-docs.com/index.html

.. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: http://s3splitmerge.my-docs.com/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Source_Code-blue.svg
    :target: http://s3splitmerge.my-docs.com/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/s3splitmerge-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/s3splitmerge-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/s3splitmerge-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/s3splitmerge#files


Welcome to ``s3splitmerge`` Documentation
==============================================================================


Features
------------------------------------------------------------------------------

Split:

- split big data file on (>=500MB) in common data format CSV, TSV, JSON into


.. _install:

Install
------------------------------------------------------------------------------

``pip install awswrangler==2.10.0 --no-deps``


``s3splitmerge`` is released on PyPI, so all you need is:

.. code-block:: console

    $ pip install s3splitmerge

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade s3splitmerge


Merge Multiple AWS S3 Json File into One Big



1. Input Data
------------------------------------------------------------------------------

Files::

    s3://my-bucket/input/date=2000-01-01/a.json # 6MB
    s3://my-bucket/input/date=2000-01-01/b.json # 600MB
    s3://my-bucket/input/date=2000-01-01/c.json # 120MB
    s3://my-bucket/input/date=2000-01-02/...
    ...

Content::

    {"id": 1, "value": "a", ...}
    {"id": 2, "value": "b", ...}
    {"id": 3, "value": "c", ...}


2. Normalize file size to approximately 6MB. If smaller than 6MB, keep it as it is::

    s3://my-bucket/input-normalized/date=2000-01-01/a-1.json # 6MB

    s3://my-bucket/input-normalized/date=2000-01-01/b-1.json # 6MB
    s3://my-bucket/input-normalized/date=2000-01-01/b-2.json # 6MB
    ...
    s3://my-bucket/input-normalized/date=2000-01-01/b-100.json # 6MB

    s3://my-bucket/input-normalized/date=2000-01-01/c-1.json # 6MB
    s3://my-bucket/input-normalized/date=2000-01-01/c-2.json # 6MB
    ...
    s3://my-bucket/input-normalized/date=2000-01-01/c-20.json # 6MB

3. Performance per file ETL using AWS Lambda::

    s3://my-bucket/output-normalized/date=2000-01-01/a-1.parquet # 6MB

    s3://my-bucket/output-normalized/date=2000-01-01/b-1.parquet # 6MB
    s3://my-bucket/output-normalized/date=2000-01-01/b-2.parquet # 6MB
    ...
    s3://my-bucket/output-normalized/date=2000-01-01/b-100.parquet # 6MB

    s3://my-bucket/output-normalized/date=2000-01-01/c-1.parquet # 6MB
    s3://my-bucket/output-normalized/date=2000-01-01/c-2.parquet # 6MB
    ...
    s3://my-bucket/output-normalized/date=2000-01-01/c-20.parquet # 6MB

4. Merge file into Bigger one for better Athena Query performance::

    s3://my-bucket/output-normalized/date=2000-01-01/part-1.parquet # 500MB
    s3://my-bucket/output-normalized/date=2000-01-01/part-2.parquet # 500MB
    ...
