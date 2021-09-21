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

* Queries Okapi-based modules and transforms JSON data for easier
  reporting
* Full SQL query support on transformed data, using an embedded
  database
* No LDP server needed; only Python, and Okapi access to send CQL
  queries
* Supports DuckDB, PostgreSQL, and Redshift database systems
* PostgreSQL/Redshift support enables:
  * Sharing the data in a multiuser database
  * Querying the data from within the LDP query builder app
  * Storing the data in an existing LDP database if available

To install LDLite or upgrade to the latest version:
```bash
$ python3 -m pip install --upgrade ldlite
```
To extract and transform data:
```
$ python3
```
```python
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
>>> ld.select(table='g_j', limit=10)
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
>>> ld.to_csv(table='g_j', filename='groups.csv')
```


More examples
-------------

* [An example running in Jupyter
Notebook](https://github.com/library-data-platform/ldlite/blob/main/examples/example.md)

* [Loading sample data from
folio-snapshot](https://github.com/library-data-platform/ldlite/blob/main/examples/snapshot.py)


Documentation
-------------

[API documentation](https://library-data-platform.github.io/ldlite/ldlite.html)



Issues
------

[Bug reports](https://github.com/library-data-platform/ldlite/issues)


