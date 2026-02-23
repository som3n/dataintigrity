# dataintegrity

**A production-grade Python SDK for data integrity, quality scoring, PII detection, statistical drift detection, and dataset versioning.**

> v0.2 — State-aware data integrity system with historical comparison engine

---

## Features

| Capability | Status |
|---|---|
| CSV & PostgreSQL connectors | ✅ |
| Large CSV safety (auto-sampling > 50 MB) | ✅ |
| Column normalisation | ✅ |
| Schema contract enforcement | ✅ |
| PII detection (email, phone, SSN, CC) | ✅ |
| Completeness, Uniqueness, Validity, Consistency, Timeliness rules | ✅ |
| Composite DataScore (weighted, 0–100) | ✅ |
| SHA-256 dataset fingerprint | ✅ |
| Statistical drift detection (KS test) | ✅ |
| **Dataset versioning (v0.2)** | ✅ |
| **Local baseline store (v0.2)** | ✅ |
| **Historical comparison engine (v0.2)** | ✅ |
| **Score delta + severity tracking (v0.2)** | ✅ |
| CLI audit command | ✅ |
| **CLI --track & --history flags (v0.2)** | ✅ |

---

## Installation

```bash
# Editable install (recommended for development)
pip install -e .

# Or standard install
pip install dataintegrity
```

---

## Quick Start — CLI (v0.2)

### Single audit
```bash
dataintegrity audit sample.csv
```

### Track versions and compare
```bash
# First run — establishes baseline
dataintegrity audit sample.csv --track

# Second run — compares against baseline
dataintegrity audit sample.csv --track
```

**Example `--track` output (second run):**
```
💾  Version saved  id=a3b4c5d6e7f80001

────────────────────────────────────────────────────────────
  DELTA COMPARISON REPORT
────────────────────────────────────────────────────────────
  Previous DataScore : 89.3
  Current  DataScore : 85.1
  Delta              : -4.2  (MODERATE)
  Baseline version   : f1e2d3c4b5a60001

────────────────────────────────────────────────────────────
  DIMENSION DELTAS
────────────────────────────────────────────────────────────
  completeness     -3.50%
  uniqueness       -1.20%
  validity          0.00%
  consistency      -0.80%
  timeliness        0.00%

────────────────────────────────────────────────────────────
  DRIFT DETECTION (KS TEST)
────────────────────────────────────────────────────────────
  ⚠  Drift detected in:
      - transaction_amount
      - session_duration
────────────────────────────────────────────────────────────
```

### Show version history
```bash
dataintegrity audit sample.csv --history
```

**Example output:**
```
────────────────────────────────────────────────────────────
  VERSION HISTORY
────────────────────────────────────────────────────────────
  [ 1]  2026-02-22 10:00:00 UTC  score=89.3  id=f1e2d3c4b5a60001
  [ 2]  2026-02-22 10:15:00 UTC  score=85.1  id=a3b4c5d6e7f80001
────────────────────────────────────────────────────────────
```

### Combined flags
```bash
dataintegrity audit sample.csv --track --history
```

---

## Severity Scale

| Level | Score Drop |
|---|---|
| 🟢 STABLE | < 2 points |
| 🔵 MINOR | 2–5 points |
| 🟡 MODERATE | 5–10 points |
| 🔴 CRITICAL | ≥ 10 points |

---

## Version Storage

Versions are stored locally under `~/.dataintegrity/versions/`, one JSON file per data source:

```
~/.dataintegrity/versions/
    <sha256-of-source-path>.json   # all versions for one source
```

File structure:
```json
{
  "source": "/path/to/sample.csv",
  "versions": [
    {
      "version_id": "f1e2d3c4b5a60001",
      "timestamp": "2026-02-22T10:00:00+00:00",
      "fingerprint": "3f4a1b2c...",
      "data_score": 89.3,
      "dimension_scores": { "completeness": 0.95, "..." : "..." },
      "source": "/path/to/sample.csv"
    }
  ]
}
```

---

## Quick Start — Python SDK

### Basic audit
```python
from dataintegrity.connectors.csv import CSVConnector
from dataintegrity.core.dataset import Dataset
from dataintegrity.integrity.engine import IntegrityEngine

connector = CSVConnector("sample.csv")
connector.connect()
df = connector.fetch()

dataset = Dataset(df, source="sample.csv")
result = IntegrityEngine().run(dataset)
print(f"DataScore: {result['data_score']}")
```

### Versioning and comparison
```python
from dataintegrity.core.versioning import DatasetVersion
from dataintegrity.core.store import LocalVersionStore
from dataintegrity.integrity.comparison import IntegrityComparator

store = LocalVersionStore()

# After running the engine:
version = DatasetVersion(dataset)  # captures score + fingerprint
store.save(version)

# On next run:
previous = store.load_latest("sample.csv")
current = DatasetVersion(new_dataset)

comparator = IntegrityComparator()
report = comparator.compare_versions(current, previous)
print(report)
# {
#   "score_delta": -4.2,
#   "dimension_deltas": {"completeness": -0.035, ...},
#   "drifted_columns": ["transaction_amount", "session_duration"],
#   "severity": "moderate",
#   ...
# }
```

### Large file safety
```python
# Auto-samples if > 50 MB (logged as WARNING)
connector = CSVConnector("huge.csv")
connector.connect()
df = connector.fetch()

# Explicit sampling
connector = CSVConnector("huge.csv", sample_size=50_000)

# Chunked streaming
connector = CSVConnector("huge.csv", chunk_size=10_000, sample_size=50_000)
```

---

## Package Structure (v0.2)

```
dataintegrity/
├── connectors/
│   ├── base.py             # Abstract connector
│   ├── csv.py              # CSV connector (+ large-file safety)
│   └── postgres.py         # PostgreSQL connector
├── ingestion/
│   ├── normalizer.py       # Column normalisation
│   ├── pii.py              # PII detection
│   └── schema_contract.py  # Schema enforcement
├── integrity/
│   ├── rules.py            # Quality rules (5 dimensions)
│   ├── scorer.py           # DataScorer (weighted aggregation)
│   ├── engine.py           # IntegrityEngine (orchestration)
│   └── comparison.py       # IntegrityComparator (NEW v0.2)
├── drift/
│   └── ks.py               # KS-test drift detection
├── core/
│   ├── config.py           # IntegrityConfig
│   ├── dataset.py          # Dataset abstraction
│   ├── hashing.py          # SHA-256 fingerprinting
│   ├── versioning.py       # DatasetVersion (NEW v0.2)
│   └── store.py            # LocalVersionStore (NEW v0.2)
└── cli.py                  # CLI (+ --track, --history)
```

---

## Configuration

```python
from dataintegrity.core.config import IntegrityConfig

config = IntegrityConfig(
    drift_p_threshold=0.01,          # Stricter drift detection
    timeliness_max_age_days=7,       # Data must be < 7 days old
    score_weights={
        "completeness": 0.40,
        "uniqueness":   0.20,
        "validity":     0.20,
        "consistency":  0.10,
        "timeliness":   0.10,
    },
)
```

---

## All CLI Options

```
Usage: dataintegrity audit [OPTIONS] FILEPATH

Options:
  --encoding TEXT        CSV file encoding.  [default: utf-8-sig]
  --delimiter TEXT       CSV column delimiter.  [default: ,]
  --no-normalize         Skip column-name normalisation.
  --json-output          Print results as JSON instead of formatted text.
  --pii-threshold FLOAT  Drift p-value threshold.  [default: 0.05]
  --sample-size INT      Max rows to read (large CSV safety).
  --track                Save version and compare to previous.
  --history              Show version history for this source.
  --help                 Show this message and exit.
```

---

## Roadmap (post-v0.2)

- [ ] JSON / Parquet / S3 connectors
- [ ] API server (FastAPI)
- [ ] Monitoring dashboard
- [ ] Rule extensibility framework
- [ ] JWT-secured audit certification
- [ ] Slack / webhook alerting

---

## License

MIT
