# dataintegrity

**A production-grade Python SDK for data integrity, quality scoring, PII detection, statistical drift detection, and reproducible auditing.**

> v0.3.1 — Governance-Grade PII Enforcement and Priority-Based Resolution

---

## Features

| Capability | Status |
|---|---|
| CSV & PostgreSQL Connectors | ✅ |
| Database Audits | ✅ |
| Large CSV Safety (auto-sampling) | ✅ |
| Column Normalisation | ✅ |
| Schema Contract Enforcement | ✅ |
| **Governance PII Detection (v0.3.1)** | ✅ |
| **Priority-Based Resolution (v0.3.1)** | ✅ |
| **FP Guardrails & Entropy Checks (v0.3.1)** | ✅ |
| Completeness, Uniqueness, Validity, Consistency, Timeliness Rules | ✅ |
| Rule Plugin System | ✅ |
| Risk-Weighted DataScore | ✅ |
| **Dataset Fingerprinting (v0.3.0)** | ✅ |
| **YAML Policy Support (v0.3.0)** | ✅ |
| Deterministic Execution Manifests | ✅ |
| Configuration Hashing | ✅ |
| Local Audit History Tracking | ✅ |
| Dataset Versioning & Drift Detection | ✅ |
| CLI Audit Tool (with JSON output) | ✅ |

---

## Installation

```bash
# Recommended: Editable install for development
pip install -e ".[dev]"

# Direct install - will be added soon
pip install dataintegrity
```

---

## Quick Start — CLI

### 1. File-based Audit
```bash
# Basic CSV audit
dataintegrity audit sample.csv

# Audit with a custom YAML policy
dataintegrity audit sample.csv --policy-file my_policy.yaml
```

### 2. Database Audit
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

## Architectural Hardening

### Structured Fingerprinting (v0.3.0)
Datasets now carry a multi-layered cryptographic fingerprint:
* **Structural**: Hash of sorted column names and dtypes.
* **Statistical**: Hash of numeric summaries (mean, std, 25/50/75th percentiles).
* **Row Count**: Total row count for basic volume verification.
* **Combined**: A final root hash of all components for absolute data lineage.

### Deterministic Manifests
Every run produces an `ExecutionManifest` (record) or `.manifest.json` file. This captures:
* **Environment Fingerprint**: OS, Python, and exact library versions (Pandas, NumPy, etc.).
* **Config Hash**: A SHA-256 hash of your settings to ensure audits are reproducible.

```bash
# Fail CI if data doesn't meet organizational overheads
dataintegrity audit data.csv --policy-file quality_gate.yaml
```

### Governance PII Enrichment (v0.3.1)
The PII engine has been upgraded to meet production-ready governance standards:
* **Priority Resolution**: Overlapping matches (e.g., Credit Card vs Aadhaar) are resolved using a deterministic priority registry.
* **Deterministic Sampling**: For massive datasets, PII scanning automatically switches to deterministic row-based sampling to maintain performance.
* **Risk Aggregation**: PII findings now provide column-level `highest_risk` and `match_ratio` metrics.
* **False Positive Guardrails**: Integrated checks for sequential numbers, repeating digits, and Shannon entropy boost precision.
* **Policy Enforcement**: YAML policies now support `block_high_risk`, `max_medium_risk_ratio`, and `allow_low_risk` rules.

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
print(f"Fingerprint: {result.fingerprint['combined']}")
```

---

## Package Structure

```
dataintegrity/
├── core/
│   ├── config_hashing.py   # Deterministic hashing
│   ├── execution.py        # ExecutionManifests
│   ├── result_schema.py    # Structured DatsetAuditResult (v0.3)
│   └── hashing.py          # Structured fingerprinting (v0.3)
├── integrity/
│   ├── plugins.py          # Rule plugin framework
│   ├── risk_model.py       # Severity-based weighting
│   ├── history.py          # Local history tracking
│   └── ... ( rules, scorer, engine )
├── policies/               # Built-in and File-based Policies
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
  --policy TEXT          Enforce a built-in policy (research, production).
  --policy-file PATH     Path to a YAML policy file.
  --save-manifest        Save execution metadata to file.
  --save-history         Record run to local history store.
  --track                Save version and compare to baseline.
  --history              Show version history for this source.
  --help                 Show this message and exit.
```

---

## License

MIT
