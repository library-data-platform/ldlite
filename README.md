LDLite
======

Copyright (C) 2021 The Open Library Foundation.  This software is
distributed under the terms of the Apache License, Version 2.0.  See
the file
[LICENSE](https://github.com/library-data-platform/ldlite/blob/master/LICENSE)
for more information.

LDLite is a lightweight, open source reporting tool for Okapi-based
services.  It is part of the Library Data Platform project and
provides basic LDP functions without requiring the server to be
installed.

To install LDLite or upgrade to the latest version:
```bash
$ python -m pip install --upgrade ldlite
```
(On some systems it might be `python3` rather than `python`.)

To extract and transform data:
```python
$ python
>>> import ldlite
>>> ld = ldlite.LDLite()
>>> ld.connect_okapi(url='https://folio-snapshot-okapi.dev.folio.org',
...                  tenant='diku',
...                  user='diku_admin',
...                  password='admin')
>>> db = ld.connect_db(filename='ldlite.db')
>>> _ = ld.query(table='g', path='/groups', query='cql.allRecords=1 sortby id')
ldlite: querying: /groups
ldlite: created tables: g, g_j, g_j_metadata
>>> ld.select(table='g_j')
```
```
 __id |                  id                  |         desc          | expiration_offset_in_days |   group   
------+--------------------------------------+-----------------------+---------------------------+-----------
    1 | 3684a786-6671-4268-8ed0-9db82ebca60b | Staff Member          |                       730 | staff     
    2 | 503a81cd-6c26-400f-b620-14c08943697c | Faculty Member        |                       365 | faculty   
    3 | ad0bc554-d5bc-463c-85d1-5562127ae91b | Graduate Student      |                           | graduate  
    4 | bdc2b6d4-5ceb-4a12-ab46-249b9a68473e | Undergraduate Student |                           | undergrad 
(4 rows)
```
```python
>>> _ = ld.query(table='u', path='/users', query='cql.allRecords=1 sortby id')
ldlite: querying: /users
ldlite: created tables: u, u_j, u_j_departments, u_j_metadata, u_j_personal, u_j_proxy_for
>>> cur = db.cursor()
>>> _ = cur.execute("""
...     CREATE TABLE user_groups AS
...     SELECT u_j.id, u_j.username, g_j.group
...         FROM u_j
...             JOIN g_j ON u_j.patron_group = g_j.id;
...     """)
>>> ld.to_csv(table='user_groups', filename='user_groups.csv')
```


Features
--------

* Queries Okapi-based modules and transforms JSON data for easier
  reporting
* Full SQL query support on transformed data, using an embedded
  database
* No LDP server needed; only Python, and Okapi access to send CQL
  queries
* Supports DuckDB, PostgreSQL, and Redshift database systems
* PostgreSQL/Redshift support enables:
  * Access to the data using database tools such as DBeaver
  * Sharing the data in a multiuser database
  * Querying the data from within the LDP query builder app
  * Storing the data in an existing LDP database if available


More examples
-------------

* [An example running in Jupyter
Notebook](https://github.com/library-data-platform/ldlite/blob/main/examples/example.md)

* [Loading sample data from
folio-snapshot](https://github.com/library-data-platform/ldlite/blob/main/examples/snapshot.py)


Documentation
-------------

LDLite runs on Windows, macOS, and Linux.

[API documentation](https://library-data-platform.github.io/ldlite/ldlite.html)

New to Python?  [Try these learning resources.](https://www.python.org/about/gettingstarted/)


Issues
------

Report bugs at [Issues](https://github.com/library-data-platform/ldlite/issues)


