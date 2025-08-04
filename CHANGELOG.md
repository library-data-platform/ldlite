# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Please see [MIGRATING.md](./MIGRATING.md) for information on breaking changes.

## [Unreleased]

### Added
- New flag to remove the raw json tables after loading is complete

### Added

- Deprecation notice for xlsx and sqlite functionality. They will be removed in 4.0.

### Changed

- Fixed endpoints that use with perPage instead of limit
- Improved the performance and stability for the download portion of LDLite
- Fixed `connect_db` method

### Removed


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
