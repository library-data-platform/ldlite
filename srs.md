Using LDLite with SRS MARC data
===============================

This page summarizes experimental use of LDLite to report on MARC data
retrieved from Source Record Storage (SRS).  The sequence of steps
below covers querying SRS data and using
[ldpmarc](https://github.com/library-data-platform/ldpmarc) to
transform the data to tabular format for easier querying via SQL.  The
suggested process assumes PostgreSQL is being used for the reporting
database.

The following requires ldpmarc v1.6.0-beta5 or later.


Querying and retrieving SRS data
--------------------------------

We will assume that LDLite has been initialized and configured:

```python
ld = ldlite.LDLite()
# etc.
```

A basic query that will retrieve SRS data is:

```python
ld.query(table='folio_source_record.records', path='/source-storage/records', json_depth=2, limit=1000)
```

In this example, the *limit* parameter has been used to reduce the
number of records retrieved.  Alternatively, SRS allows filtering by
`snapshotId`, `recordType`, or `state`, for example:

```python
ld.query(table='folio_source_record.records', path='/source-storage/records', query={'state': 'OLD'}, json_depth=2)
```

If the total number of SRS records is small, it may be possible to
retrieve all of the records; but this could be prohibitively slow.

The *json_depth* parameter should be set to 2, which will cause the
transformation to stop when it reaches the MARC JSON record and to
write the JSON object as a whole.

After this query has completed, `folio_source_record.records__t`
should contain the MARC JSON data and metadata.


Adjustments to work with ldpmarc
--------------------------------

The ldpmarc tool expects certain tables and columns to be present.  We
can create them from `folio_source_record.records__t`:

```sql
DROP TABLE IF EXISTS folio_source_record.records_lb;

CREATE TABLE folio_source_record.records_lb AS
    SELECT __id,
           id::uuid,
           state,
           matched_id::uuid,
           external_ids_holder__instance_id::uuid AS external_id,
           external_ids_holder__instance_hrid AS external_hrid
        FROM folio_source_record.records__t;

CREATE INDEX ON folio_source_record.records_lb (__id);

CREATE INDEX ON folio_source_record.records_lb (id);

CREATE INDEX ON folio_source_record.records_lb (state);

CREATE INDEX ON folio_source_record.records_lb (matched_id);

CREATE INDEX ON folio_source_record.records_lb (external_id);

CREATE INDEX ON folio_source_record.records_lb (external_hrid);

DROP TABLE IF EXISTS folio_source_record.marc_records_lb;

CREATE TABLE folio_source_record.marc_records_lb AS
    SELECT __id,
           id::uuid,
           parsed_record__content AS content
        FROM folio_source_record.records__t;

CREATE INDEX ON folio_source_record.marc_records_lb (__id);

CREATE INDEX ON folio_source_record.marc_records_lb (id);

CREATE INDEX ON folio_source_record.marc_records_lb (content);
```


Running ldpmarc
---------------

The data now should be compatible with ldpmarc.  Before continuing,
see the [ldpmarc readme
file](https://github.com/library-data-platform/ldpmarc/blob/main/README.md)
for installation and usage documentation, but note that LDP1 and
Metadb are not required for this process.

When we run ldpmarc with the `-M` option (below), it will look for
database connection parameters in a configuration file called
`metadb.conf` located within a Metadb data directory.  If the data
directory is called, for example, `data/`, then `data/metadb.conf`
should contain settings in the form:

```ini
[main]
host = <hostname>
port = 5432
database = <ldlite_database_name>
systemuser = <username>
systemuser_password = <password>
sslmode = <require_or_disable>
```

Then to run ldpmarc:

```bash
ldpmarc -D data -M -f
```

This should create a new table `folio_source_record.marctab`
containing the MARC records in a form such as:

```
                srs_id                | line |              matched_id              | instance_hrid |             instance_id              | field | ind1 | ind2 | ord | sf |                 content
--------------------------------------+------+--------------------------------------+---------------+--------------------------------------+-------+------+------+-----+----+------------------------------------------
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    1 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 000   |      |      |   1 |    | 00457nca a2200181 c 4500
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    2 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 001   |      |      |   1 |    | 354326643
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    3 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 003   |      |      |   1 |    | DE-101
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    4 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 005   |      |      |   1 |    | 20171202045622.0
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    5 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 007   |      |      |   1 |    | q|
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    6 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 008   |      |      |   1 |    | 050609s1980    ||||||||r||||||||||||||
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    7 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 016   | 7    |      |   1 | 2  | DE-101
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    8 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 016   | 7    |      |   1 | a  | 354326643
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |    9 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 016   | 7    |      |   2 | 2  | DE-101c
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   10 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 016   | 7    |      |   2 | a  | 596503000
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   11 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 035   |      |      |   1 | a  | (DE-599)DNB354326643
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   12 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 035   |      |      |   2 | a  | (OCoLC)724812418
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   13 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 040   |      |      |   1 | a  | 9999
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   14 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 040   |      |      |   1 | b  | ger
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   15 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 040   |      |      |   1 | c  | DE-101
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   16 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 040   |      |      |   1 | d  | 9999
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   17 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 100   | 1    |      |   1 | a  | Mason, Benjamin
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   18 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 100   | 1    |      |   1 | e  | Komponist
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   19 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 100   | 1    |      |   1 | 4  | cmp
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   20 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 245   | 1    | 0    |   1 | a  | Bread and water
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   21 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 773   | 0    | 8    |   1 | w  | (DE-101)354326635
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   22 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 999   | f    | f    |   1 | i  | fef9f415-1b35-3e30-89cc-17857a611338
 14ea8ed4-672b-11eb-8681-aed9fae510e9 |   23 | 14ea8ed4-672b-11eb-8681-aed9fae510e9 |               | fef9f415-1b35-3e30-89cc-17857a611338 | 999   | f    | f    |   1 | s  | 14ea8ed4-672b-11eb-8681-aed9fae510e9
```
