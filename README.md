LDLite
======

Copyright (C) 2021-2022 The Open Library Foundation. This software is
distributed under the terms of the Apache License, Version 2.0. See
the file
[LICENSE](https://github.com/library-data-platform/ldlite/blob/master/LICENSE)
for more information.

LDLite is a lightweight, open source reporting tool for FOLIO
services. It is part of the Library Data Platform project and
provides basic LDP functions without requiring the server to be
installed.

To install LDLite or upgrade to the latest version:

```bash
$ python -m pip install --upgrade ldlite
```

(On some systems it might be `python3` rather than `python`.)
Check out the [migration guide](./MIGRATING.md) for more information about major version upgrades.

To extract and transform data:

```python
$ python
>> > import ldlite
>> > ld = ldlite.LDLite()
>> > ld.connect_folio(url='https://folio-etesting-snapshot-kong.ci.folio.org',
                      tenant='diku',
                      user='diku_admin',
                      password='admin')
>> > db = ld.connect_db()
>> > _ = ld.query(table='g', path='/groups', query='cql.allRecords=1 sortby id')
ldlite: querying: / groups
ldlite: created
tables: g, g__t, g__tcatalog
>> > ld.select(table='g__t')
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
>> > _ = ld.query(table='u', path='/users', query='cql.allRecords=1 sortby id')
ldlite: querying: / users
ldlite: created
tables: u, u__t, u__t__departments, u__t__personal__addresses, u__t__proxy_for, u__tcatalog
>> > cur = db.cursor()
>> > _ = cur.execute("""
        CREATE TABLE user_groups AS
        SELECT u__t.id, u__t.username, g__t.group
            FROM u__t
                JOIN g__t ON u__t.patron_group = g__t.id;
        """)
>> > ld.export_excel(table='user_groups', filename='groups.xlsx')
```

Features
--------

* Queries FOLIO modules and transforms JSON data into tables for
  easier reporting
* Full SQL query support and export to CSV or Excel
* Compatible with DBeaver database tool
* Compatible with DuckDB and PostgreSQL database systems
* PostgreSQL support enables:
    * Sharing the data in a multiuser database
    * Access to the data using more database tools
    * Storing the data in an existing LDP database if available
* Runs on Windows, macOS, and Linux.

More examples
-------------

* [An example running in Jupyter
  Notebook](https://github.com/library-data-platform/ldlite/blob/main/examples/example.md)

* [Loading sample data from FOLIO demo
  sites](https://github.com/library-data-platform/ldlite/blob/main/examples/folio_demo.py)

* [Using LDLite with SRS MARC data](https://github.com/library-data-platform/ldlite/blob/main/srs.md)

LDLite resources
----------------

* [LDLite API documentation](https://library-data-platform.github.io/ldlite/ldlite.html)

* The LDP project runs a Slack workspace which is a good place to ask
  questions or to share your work. It also serves as a community space
  for working together on library data problems. To request an invitation,
  use the [Contact page](https://librarydataplatform.org/contact/)
  on the LDP website.

* Report bugs at [Issues](https://github.com/library-data-platform/ldlite/issues)

Other resources
---------------

* [FOLIO API documentation](https://dev.folio.org/reference/api/)

* [Python learning resources](https://www.python.org/about/gettingstarted/)
