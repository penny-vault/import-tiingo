# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [0.2.0] - 2022-05-28
### Added
- Assets to download can now be limited by type.
- `hide-progress` flag will suppress the progress bar output (useful when redirecting stdout/stderr to a log file)

### Changed
- history time display shown in an easier to read format

## [0.1.0] - 2022-05-20
### Added
- Download stocks and etf's from Polygon
- Download mutual funds from Tiingo
- Enrich assets with Industry/Sector from yFinance!
- Encrich assets with Composite FIGI from Openfigi mapping API
- Save changed assets to database and backblaze

[Unreleased]: https://github.com/penny-vault/import-tiingo/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/penny-vault/import-tiingo/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/penny-vault/import-tiingo/releases/tag/v0.0.1
