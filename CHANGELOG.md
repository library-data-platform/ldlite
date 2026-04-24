# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Please see [MIGRATING.md](./MIGRATING.md) for information on breaking changes.

## [Unreleased]

### Added

### Fixed

### Changed

### Removed

## [4.0.1] - April

### Changed
* Relaxed datetime detection from ~rfc3339 to ~iso8601

## [4.0.0] - April

### Fixed
* Invalid UTF8 strings when transforming json
* Logging raw table name when keep_raw=False

### Added
* Support for python 3.14 and duckdb 1.14+
* ldlite_system.load_history_v1 table which tracks useful information about data loads
* Generated columns are now indexed when using duckdb

### Changed
* [Possibly Breaking] Expanding json now utilizes SQL instead of python
* [Possibly Breaking] Columns have more accurate types
  * text -> timestamptz
  * numeric -> numeric or bigint or integer
* [Breaking] The connect_db_postgres method now returns a psycopg3 connection
* [Possibly Breaking] `select` method uses duckdb's `show` method
* [Possibly Breaking] `export_csv` method uses duckdb's `COPY TO output.csv`

### Removed
* [Breaking] Support for dependency versions less than the new minimum version
  * Postgres - 14
  * Python - 3.10
  * Duckdb - 1.3
* [Breaking] experimental_connect_sqlite
* [Breaking] export_excel and to_xlsx

## [3.2.1] - November 2025

### Fixed

- Endpoints which return system fields before the record list
- Example python script
- Columns with mixed and incompatible datatypes
- Endless looping for calendar/calendars endpoint

## [3.2.0] - September 2025

### Added

- Source Storage endpoints now stream only if streaming is available.
- Connections returned from the LDLite.connect_db methods are now isolated from the ones used internally.

### Changed

- psycopg3 is now used for internal operations. LDLite.connect_db_postgres will return a psycopg3 connection instead of psycopg2 in the next major release.
- psycopg2 is now installed as the binary version.
- Refactored internal table management to be safer and more resilient.
- Ingesting data into postgres now uses COPY FROM which significantly improves the download performance.

### Removed

## [3.1.4] - September 2025

### Fixed
- Fixed `connect_db` method return value for default :memory: connections
- Fixed flaky end to end test for source records in the snapshot environment

## [3.1.3] - September 2025

### Fixed
- Fixed multiple sequential newline characters in srs download
- Fixed edge cases around non-default sorts and pagination
- Unexposed an accidentally exposed internal module

## [3.1.2] - August 2025

### Fixed
- Fixed an issue when FOLIO returned null a record first which interacted poorly with the offset fix

## [3.1.1] - August 2025

### Fixed
- Now falling back to offset based paging when there is no id field in the response
- Fixed an issue when FOLIO was returning null records (why is this happening in the first place?)

## [3.1.0] - August 2025

### Added

- Deprecation notice for xlsx and sqlite functionality. They will be removed in 4.0.
- pdm scripts for locking and testing installability
- New lockfile for testing python against 3.13
- New flag to remove the raw json tables after loading is complete

### Fixed

- Fixed endpoints that use with perPage instead of limit
- Fixed `connect_db` method return value
- Re-enabled indexing for postgres databases

### Changed

- Improved the performance and stability for the download portion of LDLite

## [3.0.0] - July 2025

### Added

- Python type hint support
- 80% test coverage
- Ruff and Mypy for code quality

### Fixed

- `LDLite.drop_tables` method
- UUID columns for postgres databases were loaded as varchar
- Incorrect datatypes were exported to csv

### Changed

- [Possibly Breaking] Unpinned dependency versions
- [Possibly Breaking] CSV output format is now deterministic with columns in alphabetic order

### Removed

- [Breaking] Support for eol versions of python <3.9
- [Possibly Breaking] Unnecessary direct dependency on pandas
