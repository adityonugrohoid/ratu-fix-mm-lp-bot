# Changelog

## Unreleased
### Refactored
- Defensive upgrade to FIX message parser in `fix_connector.py`. The parser now robustly handles malformed, partial, and error cases without crashing or emitting invalid messages.

## 1.0.1 - 2025-05-08
### Removed
- Removed the references for `auto-reconnect` in the dropcopy session to fix the following [issue](https://github.com/binance/binance-fix-connector-python/issues/2).

## 1.0.0 - 2025-03-24
### Added
- First release, details in the `README.md` file
