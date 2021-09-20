LDLite
======

Copyright (C) 2021 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0.  See the file
[LICENSE](https://github.com/library-data-platform/ldlite/blob/master/LICENSE)
for more information.

LDLite is a lightweight reporting tool for Okapi-based services.  It
is part of the Library Data Platform project and provides basic LDP
functions without requiring the platform to be installed.


Features
--------

* LDLite provides very lightweight LDP functions, with no
  infrastructure needed.  It requires only Python, and Okapi access to
  send CQL queries.

* Full SQL query support.  By default it uses an embedded DuckDB
  analytic database stored in a local file (no set up needed), and
  also supports PostgreSQL as an option.

* Easy installation: `pip install ldlite`

* Works with Jupyter Notebook and Python's data science ecosystem.

* Open source


Example
-------

[An example running in Jupyter Notebook](example/example.md)


Documentation
-------------

[API reference documentation](https://library-data-platform.github.io/ldlite/ldlite.html)



Installing
----------

To install LDLite or upgrade to the latest version:

```
python3 -m pip install --upgrade ldlite
```


