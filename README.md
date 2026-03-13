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

LDLite supports two modes of usage.
* Server mode uses a persistent postgres server and managed cron server to download large amounts of data for analytic processing and reporting.
* Ad-hoc mode uses a local DuckDB database to enable downloading small amounts of data and querying using sql.

### Usage with a persistent postgres server

See the [Five Colleges Setup](https://github.com/Five-Colleges-Incorporated/ldlite-scripts) for an example of automating overnight data loads.

It is recommended to install the `psycopg[c]` package for optimal reliability and performance in a server context.

### Usage for ad-hoc local querying

To install LDLite or upgrade to the latest version:

```bash
$ python -m pip install --upgrade psycopg[binary] ldlite
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
>> > _ = ld.query(table='g', path='/groups')
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
>> > _ = ld.query(table='u', path='/users')
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
>> > ld.export_csv(table='user_groups', filename='groups.csv')
```

Features
--------

* Queries FOLIO modules and transforms JSON data into tables for
  easier reporting
* Full SQL query support and export to CSV
* Compatible with DBeaver database tool
* Compatible with DuckDB and PostgreSQL database systems
* PostgreSQL support enables:
    * Sharing the data in a multiuser database
    * Access to the data using more database tools
    * Storing the data in an existing LDP database if available
* Runs on Windows, macOS, and Linux.

ldlite_system.load_history_v1
-------------

Starting with ldlite 4.0 useful information is stored during the loading process.
This table can be exposed directly to end users but it can be overwhelming.
A more useful view of this table can be exposed instead.
```sql
CREATE VIEW public.load_history_dashboard AS
SELECT
  table_prefix
  ,folio_path
  ,COALESCE(query_text, 'cql.allRecords=1') AS query_text
  ,final_rowcount AS rowcount
  ,pg_size_pretty(SUM(t.table_size)) AS total_size
  ,TO_CHAR(data_refresh_start AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI') AS data_refresh_start
  ,TO_CHAR(data_refresh_end AT TIME ZONE 'America/New_York', 'YYYY-MM-DD HH24:MI') AS data_refresh_end
FROM ldlite_system.load_history_v1 h
CROSS JOIN LATERAL
(
  SELECT pg_total_relation_size(t."table_schema" || '.' || t."table_name") AS table_size
  FROM INFORMATION_SCHEMA.TABLES t
  WHERE
  (
    h.table_prefix LIKE '%.%' AND
    t.table_schema = SPLIT_PART(h.table_prefix, '.', 1) AND
    t.table_name LIKE (SPLIT_PART(h.table_prefix, '.', -1) || '%')
  ) OR
  (
    h.table_prefix NOT LIKE '%.%' AND
    t.table_name LIKE (h.table_prefix || '%')
  )
) t
GROUP BY 1, 2, 3, 4, 6, 7
```

When a load starts the table_prefix, folio_path, query_text, and load_start columns are set.
Any existing loads with the same table_prefix will have these values overwritten.

The download will transactionally replace the existing raw table and set the rowcount and download_complete fields.

The transformation will transactionally replace the expanded tables. If it fails the existing tables will be retained.
At the end of transformation, in the same transaction, the final_rowcount and transform_complete columns are set.

The data_refresh_start and data_refresh_end times require special attention.
These columns get updated when the transformation transaction is committed and represent when the download started and ended.
Any changes in FOLIO made before data_refresh_start will be reflected in the expanded tables.
Any changes in FOLIO made after data_refresh_end will not be reflected in the expanded tables.
Changes made to FOLIO in between the start and end _may_ be reflected :smile_cat:/:scream_cat:.

Because of the transactional nature, it is very possible to have newer data in the raw table than in the resulting expanded tables.
This can happen during the transformation stage or if the transformation stage fails.
This is indicated by having the data_refresh_start and data_refresh_end columns not match the load_start and download_complete columns.

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
