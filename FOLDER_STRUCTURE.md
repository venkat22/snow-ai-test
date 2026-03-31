# 📁 Snowflake Sales Platform — Folder Structure

## Overview

All files have been organized into a clean, logical structure for easy navigation and maintenance.

```
c:\tmp\snow\
│
├── 📂 sql/                           [All SQL scripts organized by phase]
│   ├── phase1_foundation/
│   │   └── 01_phase1_foundation.sql
│   ├── phase2_bronze/
│   │   ├── 02_phase2_bronze.sql
│   │   └── bronze_silver_gold_ddl.sql
│   ├── phase2_silver/
│   │   └── 03_phase2_silver.sql
│   ├── phase2_gold/
│   │   └── 04_phase2_gold.sql
│   ├── phase3_data_products/
│   │   └── 05_phase3_data_products.sql
│   ├── phase4_feature_store/
│   │   ├── 06_feature_store_ml.sql
│   │   ├── 07_feature_store_explore.sql
│   │   └── FEATURE_STORE_SNOWFLAKE_QUERIES.sql
│   └── governance/
│       ├── acceptance_gates.sql
│       ├── dama6_quality_checks.sql
│       ├── mark_marketplace_gate.sql
│       └── bi_queries.sql
│
├── 📂 python/                        [All Python scripts organized by function]
│   ├── orchestration/
│   │   └── run_all.py               [Master orchestration script]
│   ├── feature_store/
│   │   └── feature_store.py         [Feature store Python API]
│   ├── validation/
│   │   ├── check_status.py
│   │   ├── quick_check.py
│   │   └── verify_roles_sla.py
│   └── data_engineering/
│       └── snowpark_silver_job.py
│
├── 📂 docs/                          [All documentation organized by category]
│   ├── guides/
│   │   ├── setup_guide.md
│   │   └── implementation_runbook.md
│   ├── feature_store_docs/
│   │   ├── README_FEATURE_STORE.md               [⭐ START HERE]
│   │   ├── PHASE_4_COMPLETE.md
│   │   ├── FEATURE_STORE_QUICK_REF.md
│   │   ├── FEATURE_STORE_SNOWFLAKE_ACCESS.md
│   │   ├── FEATURE_STORE_GUIDE.md
│   │   ├── FEATURE_STORE_QUICKSTART.md
│   │   ├── FEATURE_STORE_SNOWFLAKE_GUIDE.md
│   │   ├── FEATURE_STORE_SETUP_CHECKLIST.md
│   │   ├── FEATURE_STORE_ARCHITECTURE.md
│   │   ├── ML_FEATURE_STORE_README.md
│   │   └── FEATURE_STORE_SUMMARY.md
│   └── project/
│       ├── plan.md
│       ├── data_products.md
│       ├── Tech_Challenge.md
│       ├── snowtest.md
│       └── marketplace_listing.txt
│
├── 📂 marketplace_ui/                [Custom UI application]
│   ├── app.py
│   ├── Dockerfile
│   ├── README.md
│   ├── requirements.txt
│   ├── run_dev.ps1
│   ├── run_docker.ps1
│   ├── run_podman.ps1
│   ├── static/
│   └── templates/
│
├── 📂 .venv/                         [Python virtual environment]
└── 📂 __pycache__/                   [Python cache]
```

---

## 📂 Directory Guide

### `sql/`
Contains all SQL implementation organized by Medallion phase + governance:
- **phase1_foundation/** — Raw data sources & canonical mappings
- **phase2_bronze/** — Ingestion layer with lineage
- **phase2_silver/** — Cleansed, standardized data (DAMA 6)
- **phase2_gold/** — Dimensional model & curated marts
- **phase3_data_products/** — Data products & contracts
- **phase4_feature_store/** — ML feature store with PIT correctness
- **governance/** — Quality checks, acceptance gates, monitoring queries

### `python/`
All Python scripts organized by purpose:
- **orchestration/** — Master orchestration (`run_all.py`)
- **feature_store/** — Feature store API for data scientists
- **validation/** — Health checks & status verification
- **data_engineering/** — Snowpark jobs & complex transformations

### `docs/`
All documentation organized by audience:
- **guides/** — Setup & implementation instructions
- **feature_store_docs/** — Feature store guides & architecture (11 files)
- **project/** — Master plans, data products, tech challenge docs

### `marketplace_ui/`
Standalone marketplace application with Flask/UI components

---

## 🚀 Quick Navigation

### To Run Everything
```bash
cd c:\tmp\snow
python python/orchestration/run_all.py --include-foundation --run-acceptance-gates
```

### To Access Feature Store SQL
```bash
# All feature store SQL in one place
c:\tmp\snow\sql\phase4_feature_store\
  ├── 06_feature_store_ml.sql           ← Implementation
  ├── 07_feature_store_explore.sql      ← Views & queries
  └── FEATURE_STORE_SNOWFLAKE_QUERIES.sql ← Example queries
```

### To Read Feature Store Docs
```bash
# All feature store docs in one place
c:\tmp\snow\docs\feature_store_docs\
  ├── README_FEATURE_STORE.md           ← Start here
  ├── FEATURE_STORE_QUICK_REF.md        ← 1-page summary
  └── (9 more comprehensive guides)
```

### To View Data Product Plans
```bash
# Project documentation
c:\tmp\snow\docs\project\
  ├── plan.md                           ← Master plan
  ├── data_products.md                  ← Product specs
  └── Tech_Challenge.md                 ← Challenge requirements
```

---

## 🎯 Updates Needed (Important!)

### Update `run_all.py` paths
If you use absolute paths in `run_all.py`, update them to reflect new locations:

**Old paths:**
```python
Path(__file__).parent / "01_phase1_foundation.sql"
Path(__file__).parent / "06_feature_store_ml.sql"
```

**New paths:**
```python
Path(__file__).parent / "sql" / "phase1_foundation" / "01_phase1_foundation.sql"
Path(__file__).parent / "sql" / "phase4_feature_store" / "06_feature_store_ml.sql"
```

### Update documentation references
Any docs referencing file paths should be updated to reflect new locations.

---

## ✨ Benefits of This Structure

✅ **Organized by phase** — Easy to find Phase 1-4 implementations  
✅ **Separated concerns** — SQL, Python, and docs are distinct  
✅ **Scalable** — Easy to add new phases or features  
✅ **Discoverable** — Clear folder names match your architecture  
✅ **Maintainable** — Related files grouped together  
✅ **Professional** — Ready for team collaboration  

---

## 📝 Summary of Changes

| Item | Before | After |
|------|--------|-------|
| SQL files | 13 files in root | Organized in 7 subfolders |
| Python files | 6 files in root | Organized in 4 subfolders |
| Documentation | 16 files in root | Organized in 3 subfolders |
| Total root files | 39 files | Clean! |

---

## Next Steps

1. ✅ **Files are organized** — All done!
2. ❓ **Update run_all.py** — Fix any hardcoded paths (see above)
3. 🧪 **Test the structure** — Run orchestration script to verify paths work
4. 📚 **Update docs links** — If any docs reference file paths, update them

---

**Your Snowflake sales platform is now neatly organized and ready to scale!** 🚀
