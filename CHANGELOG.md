# Changelog

## [0.3.0] – Fingerprinting & YAML Policy Release

### Added
- **Output Schema Versioning**: JSON output now includes `"schema_version": "0.3"`.
- **Dataset Fingerprinting**: Structured fingerprints including structural (schema), statistical (numeric summary), and combined hashes.
- **YAML Policy Support**: New `--policy-file <path>` flag to load custom thresholds from a YAML file.
- **Deterministic Hashing**: Enhanced deterministic statistical fingerprinting for numerical columns.

### Improvements
- **CLI Policy Enforcement**: Corrected exit codes and report rendering when policies fail in JSON and pretty modes.
- **SDK Versioning**: Synchronised internal versioning across the package.

### Backward Compatibility
- No breaking changes to existing CLI flags or output fields.
- `result.to_legacy_dict()` remains unchanged.

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
