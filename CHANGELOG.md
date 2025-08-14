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
