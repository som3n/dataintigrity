# dataintegrity

**A production-grade Python SDK for data integrity, quality scoring, PII detection, statistical drift detection, and reproducible auditing.**

> v0.2.1 — Deterministic data infrastructure with risk-weighted scoring and database support

---

## Features

| Capability | Status |
|---|---|
| CSV & PostgreSQL Connectors | ✅ |
| **Database Audits (v0.2.1)** | ✅ |
| Large CSV Safety (auto-sampling) | ✅ |
| Column Normalisation | ✅ |
| Schema Contract Enforcement | ✅ |
| PII Detection (Email, Phone, SSN, etc.) | ✅ |
| Completeness, Uniqueness, Validity, Consistency, Timeliness Rules | ✅ |
| **Rule Plugin System (v0.2.1)** | ✅ |
| **Risk-Weighted DataScore (v0.2.1)** | ✅ |
| **Deterministic Execution Manifests (v0.2.1)** | ✅ |
| **Configuration Hashing (v0.2.1)** | ✅ |
| **Local Audit History Tracking (v0.2.1)** | ✅ |
| Dataset Versioning & Drift Detection | ✅ |
| CLI Audit Tool (with JSON output) | ✅ |

---

## Installation

```bash
# Recommended: Editable install for development
pip install -e ".[dev]"

# Direct install
pip install dataintegrity
```

---

## Quick Start — CLI (v0.2.1)

### 1. File-based Audit
```bash
# Basic CSV audit
dataintegrity audit sample.csv

# Audit with JSON output, manifest generation, and history tracking
dataintegrity audit sample.csv --output json --save-manifest --save-history
```

### 2. Database Audit (New in v0.2.1)
```bash
# Audit a specific table
dataintegrity audit --dsn "postgresql://user:pass@localhost:5432/db" --table users

# Audit via custom SQL query
dataintegrity audit --dsn "postgresql://user:pass@localhost:5432/db" --query "SELECT * FROM orders WHERE amount > 100"
```

### 3. Versions & Drift
```bash
# First run — baseline
dataintegrity audit sample.csv --track

# Subsequent run — compares and detects drift
dataintegrity audit sample.csv --track
```

---

## Architectural Hardening (v0.2.1)

### Deterministic Manifests
Every run produces an `ExecutionManifest` (record) or `.manifest.json` file. This captures:
* **Environment Fingerprint**: OS, Python, and exact library versions (Pandas, NumPy, etc.).
* **Config Hash**: A SHA-256 hash of your settings to ensure audits are reproducible.

### Risk-Weighted Scoring
Rules are no longer treated equally. Failures in **HIGH** severity dimensions (like Completeness) penalize the composite `DataScore` more heavily than **LOW** severity failures (like Timeliness).

### History Store
Audits are now tracked locally in `~/.dataintegrity/history/`. This enables long-term trend analysis of your data quality.

---

## Quick Start — Python SDK

### PostgreSQL Audit
```python
from dataintegrity.connectors.postgres import PostgresConnector
from dataintegrity.core.dataset import Dataset
from dataintegrity.integrity.engine import IntegrityEngine

connector = PostgresConnector(
    host="localhost", database="db", user="user", password="pass",
    query="SELECT * FROM users"
)
connector.connect()
df = connector.fetch()

dataset = Dataset(df, source="postgres:users")
result = IntegrityEngine().run(dataset)

print(f"Composite Score: {result.overall_score}")
```

### Rule Plugin System
```python
from dataintegrity.integrity.plugins import register_rule, IntegrityRule, RuleResult

@register_rule
class MyCustomRule(IntegrityRule):
    id = "custom_check"
    severity = "MEDIUM"
    
    def evaluate(self, dataset, config) -> RuleResult:
        # Custom logic here
        return RuleResult(passed=True, metric_value=1.0, ...)
```

---

## Package Structure (v0.2.1)

```
dataintegrity/
├── core/
│   ├── config_hashing.py   # Deterministic hashing
│   ├── execution.py        # ExecutionManifests
│   ├── result_schema.py    # Structured DatsetAuditResult
│   └── ... ( hshing, versioning, store )
├── integrity/
│   ├── plugins.py          # Rule plugin framework
│   ├── risk_model.py       # Severity-based weighting
│   ├── history.py          # Local history tracking
│   └── ... ( rules, scorer, engine )
├── connectors/             # CSV & Postgres
├── ingestion/              # Normalization & PII
├── drift/                  # KS-test
└── cli.py                  # Multi-source CLI
```

---

## All CLI Options

```
Usage: dataintegrity audit [OPTIONS] [FILEPATH]

Options:
  --dsn TEXT             PostgreSQL connection DSN.
  --table TEXT           Database table name to audit.
  --query TEXT           Custom SQL query to audit.
  --output [pretty|json] Output format. [default: pretty]
  --save-manifest        Save execution metadata to file.
  --save-history         Record run to local history store.
  --track                Save version and compare to baseline.
  --history              Show version history for this source.
  --help                 Show this message and exit.
```

---

## License

MIT
