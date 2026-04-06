# Snowflake AI Data Marketplace Platform

A comprehensive, end-to-end data platform built on Snowflake that demonstrates modern data engineering practices, from raw data ingestion through to AI-ready feature stores and business intelligence.

## 🚀 Quick Start

### Prerequisites
- Snowflake account with SYSADMIN or ACCOUNTADMIN role
- Access to `SNOWFLAKE_SAMPLE_DATA` (built-in TPC-H data)
- Python 3.8+ (for marketplace UI and feature store components)

### Run the Complete Demo
1. **Execute SQL Scripts** (in order):
   ```bash
   cd demo/
   # Run scripts 01-10 in Snowflake Worksheets
   ```

2. **Launch Marketplace UI**:
   ```bash
   cd marketplace_ui/
   pip install -r requirements.txt
   python app.py
   ```

3. **Explore Feature Store**:
   ```bash
   cd python/feature_store/
   python feature_store.py
   ```

## 📊 Architecture Overview

```
Raw Data (TPC-H) → Landing → Bronze (+Audit) → Silver (Quality) → Gold (Star Schema)
                      ↓           ↓              ↓              ↓
                Streams     Dynamic Tables  Data Products  BI Tables
                      ↓           ↓              ↓              ↓
                CDC         ML Features     Governance     Analytics
```

### Key Components

- **🏗️ Data Platform**: Medallion architecture with DAMA 6 quality checks
- **🛍️ Marketplace UI**: Custom portal for data product discovery and SLA monitoring
- **🤖 Feature Store**: ML-ready features with point-in-time correctness
- **📈 Analytics**: Star schema design with pre-built BI queries
- **🔒 Governance**: RBAC, acceptance gates, and audit trails

## 📁 Project Structure

```
├── demo/                    # Step-by-step SQL demo scripts
├── docs/                    # Comprehensive documentation
│   ├── diagrams/           # Architecture diagrams
│   └── guides/            # Implementation guides
├── marketplace_ui/         # Custom data marketplace portal
├── python/                 # Python components
│   ├── data_engineering/  # Snowpark jobs
│   ├── feature_store/     # ML feature engineering
│   └── validation/        # Quality checks
├── sql/                    # Organized SQL scripts by phase
└── docs/                   # Feature store documentation
```

## 🎯 Demo Scripts (40 minutes total)

| Phase | Script | Description | Runtime |
|-------|--------|-------------|---------|
| 1 | `01_setup_warehouse_and_schemas.sql` | Foundation setup | ~10s |
| 2 | `02_ingest_landing_data.sql` | Raw data ingestion | ~2 min |
| 3 | `03_bronze_layer_with_metadata.sql` | Audit trails & CDC | ~3 min |
| 4 | `04_silver_layer_dama6_quality.sql` | Data quality & cleansing | ~5 min |
| 5 | `05_gold_star_schema.sql` | Dimensional modeling | ~10 min |
| 6 | `06_data_products_and_governance.sql` | Products & RBAC | ~5 min |
| 7 | `07_feature_store_ml.sql` | ML feature store | ~10 min |
| 8 | `08_dama6_quality_checks.sql` | Quality validation | ~2 min |
| 9 | `09_bi_analytics_queries.sql` | Business intelligence | ~1 min |
| 10 | `10_acceptance_gates.sql` | Release validation | ~1 min |

## 🛠️ Key Technologies

- **Snowflake**: Data warehousing, streams, dynamic tables, tasks
- **Snowpark**: Python data processing
- **FastAPI**: Marketplace UI backend
- **DAMA 6**: Data quality framework
- **Kimball Methodology**: Dimensional modeling
- **Point-in-Time Correctness**: ML feature engineering

## 📚 Documentation

- [Implementation Runbook](docs/guides/implementation_runbook.md)
- [Setup Guide](docs/guides/setup_guide.md)
- [Feature Store Guide](docs/feature_store_docs/FEATURE_STORE_GUIDE.md)
- [Architecture Overview](docs/project/data_products.md)

## 🔧 Development

### Environment Setup
```bash
# Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt  # If created
```

### Running Components
```bash
# Marketplace UI
cd marketplace_ui/
python app.py

# Feature Store operations
cd python/feature_store/
python feature_store.py

# Validation checks
cd python/validation/
python quick_check.py
```

## 📈 Business Value

- **Data Quality**: DAMA 6 compliance with automated validation
- **Time-to-Insight**: Pre-built analytics and BI queries
- **Governance**: SLA monitoring and acceptance gates
- **ML Ready**: Feature store with versioning and lineage
- **Scalability**: Snowflake's elastic compute and storage

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run validation checks
5. Submit a pull request

## 📄 License

This project is for educational and demonstration purposes.

---

**Built with ❤️ for Snowflake data platform excellence**