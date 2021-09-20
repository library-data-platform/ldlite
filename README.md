LDLite
======

Copyright (C) 2021 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0.  See the file
[LICENSE](https://github.com/library-data-platform/ldlite/blob/master/LICENSE)
for more information.

LDLite is a lightweight, open source reporting tool for Okapi-based
services.  It is part of the Library Data Platform project and
provides basic LDP functions without requiring the server to be
installed.


Features
--------

* Queries Okapi-based modules and transforms the data for easier
  reporting

* Full SQL query support on transformed data, using an embedded
  database

* No LDP server needed; only Python, and Okapi access to send CQL
  queries

* Easy installation (`pip install ldlite`)

* Supports DuckDB, PostgreSQL, and Redshift database systems

* PostgreSQL/Redshift support enables:
  * Sharing the data in a multiuser database
  * Querying the data from within the LDP query builder app
  * Storing the data in an existing LDP database if available


Examples
--------

[An example running in Jupyter
Notebook](https://github.com/library-data-platform/ldlite/blob/main/examples/example.md)

[Load sample data from
folio-snapshot](https://github.com/library-data-platform/ldlite/blob/main/examples/snapshot.py)


Documentation
-------------

[API reference documentation](https://library-data-platform.github.io/ldlite/ldlite.html)



Installing
----------

To install LDLite or upgrade to the latest version:

```
python3 -m pip install --upgrade ldlite
```


Issues
------

[Bug reports](https://github.com/library-data-platform/ldlite/issues)


