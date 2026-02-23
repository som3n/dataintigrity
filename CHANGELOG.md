# Changelog

## [0.2.2] – Governance & Standards Release

### Added
- ISO/IEC 25012 alignment profile (`--profile iso-25012`)
- Policy engine with enforcement layer
- Built-in policies:
  - research
  - production
- CLI support for `--policy`
- Structured policy evaluation in JSON output
- CI-friendly exit codes for policy violations

### Architectural Improvements
- Clear separation between:
  - Measurement (IntegrityEngine)
  - Interpretation (Standards Alignment)
  - Enforcement (Policy Engine)

### Backward Compatibility
- No breaking changes
- Default CLI behavior unchanged
