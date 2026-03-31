# Plan: Snowflake Sales Platform (Updated Requirements Baseline)

Build a production-grade Snowflake sales platform using a Medallion plus Platform Services architecture, with TPC-H as the canonical source baseline.

Delivery model:
1. Committed scope: Foundation, Medallion, Platform Services, Data Products, Marketplace publication.
2. Committed architecture stubs: AI-ready semantic and retrieval scaffolding.
3. Stretch-only: Custom Marketplace UI.

---

## Phase 1: Foundation and Canonical Source Mapping (Days 1-3)

### Steps
1. Activate Snowflake trial and verify warehouse configuration.
2. Set canonical source baseline to `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`.
3. Create target database and schemas: `RAW_SALES.LANDING`, `BRONZE`, `SILVER`, `GOLD`, `MONITORING`.
4. Define source-to-domain mapping:
   - `CUSTOMER` -> customers
   - `ORDERS` -> orders
   - `LINEITEM` -> order_items
   - `PART` -> products
   - `SUPPLIER` and `NATION/REGION` -> supplier/territory proxy mappings
5. Document approved synthetic enrichment rules where TPC-H does not natively contain sales-rep constructs.

### Acceptance Gate
1. All downstream scripts reference the approved TPCH baseline and mapped entities only.
2. Source mapping document exists and is linked from this plan.
3. No multi-option source language remains in this plan.

---

## Architecture Blueprint: Medallion plus Platform Services

### Medallion Layers
1. Bronze: Raw-domain aligned copies with lineage metadata and immutable ingestion history.
2. Silver: Cleansed, standardized, DAMA 6-compliant canonical business entities.
3. Gold: Dimensional model plus curated marts for BI and product consumption.

### Platform Services (Committed)
1. CDC and incremental processing via Streams.
2. Incremental transformation serving via Dynamic Tables.
3. Orchestration and schedules via Tasks.
4. Complex transformation and validation via Snowpark (Python).

### Governance and Observability
1. SLA and DQ metrics persisted in `MONITORING` objects.
2. Access model enforced by role-based grants.
3. Operational health includes task success, stream lag, dynamic table refresh status, and Snowpark job outcomes.

---

## Phase 2: Medallion Implementation with Mandatory Platform Features (Days 4-14)

### Milestone 2.1 Bronze
1. Build Bronze tables from mapped TPCH entities with load metadata.
2. Add Streams where change tracking is required for downstream incremental logic.

### Milestone 2.2 Silver
1. Implement DAMA 6 quality rules and rejected-record capture.
2. Build Silver transformations using Dynamic Tables for incremental behavior.
3. Use Snowpark for non-trivial validations and standardization logic.

### Milestone 2.3 Gold
1. Complete dimensional model and required fact tables.
2. Build BI-ready aggregate marts.
3. Apply clustering and search optimization policies to high-impact tables.

### Phase 2 Acceptance Gate
1. Dynamic Tables, Streams, Tasks, and Snowpark are present and operational.
2. DAMA 6 checks produce measurable pass/fail evidence.
3. Gold model supports required BI product contracts.

---

## AI-Ready Architecture (Committed as Stubs, Not Full App)

### Scope
1. Add semantic metadata structures for business entities and definitions.
2. Add embedding-ready schema fields and retrieval index/table stubs.
3. Document RAG retrieval path assumptions and query flow using sample prompts.

### Out of Scope for This Release
1. Production RAG application UI.
2. Full model training and serving infrastructure.
3. External vector platform integration beyond stubs.

### Acceptance Gate
1. AI-ready schema stubs created.
2. Metadata is queryable and discoverable.
3. Retrieval path is documented and testable with sample SQL-level queries.

---

## Phase 3: Data Products, SLA Enforcement, and Marketplace (Days 15-20)

### Steps
1. Finalize three contract-based data products with explicit inputs, outputs, and ownership.
2. Enforce SLA dimensions: freshness, latency, completeness, accuracy, availability.
3. Implement Task-based refresh orchestration and pre-refresh quality gates.
4. Enforce role-based access and least-privilege patterns.
5. Publish Marketplace listing and validate consumer subscription/query flow.

### Acceptance Gate
1. Three products have signed-off contracts and SLA evidence.
2. Pre-refresh quality gates run and are auditable.
3. Consumer can discover, subscribe, and run documented sample queries.

---

## Phase 4: ML Feature Store — Point-in-Time Correct Features (Days 18-20; Parallel Track)

### Scope
1. Build offline feature store with **point-in-time correctness** for training data reproducibility.
2. Implement feature versioning and lineage tracking.
3. Create entity-keyed feature tables (customer, product, sales rep).
4. Publish feature registry and provide Python API for data scientists.
5. Design for future online store expansion (stubs).

### Key Components

#### Offline Store Tables
1. **Customer RFM Features** (`customer_rfm_features_offline`) — Recency, Frequency, Monetary with historical accuracy
2. **Customer Engagement Features** (`customer_engagement_features_offline`) — Churn indicators, LTV, customer health scores
3. **Product Performance Features** (`product_performance_features_offline`) — Revenue, volume, quality metrics by date
4. **Sales Rep Quota Features** (`sales_rep_quota_features_offline`) — Quota attainment, KPIs by date

#### Governance & Metadata
1. **Feature Registry** (`feature_registry`) — Master catalog with ownership, tags, lineage, descriptions
2. **Feature Versioning** (`feature_versions`) — Track all schema changes, deployments, rollbacks
3. **Feature Lineage** (`feature_lineage`) — Dependency graph for impact analysis and change management

#### Developer Experience
1. **Python API** (`feature_store.py`) — Simple interface: `get_customer_features_as_of()`, `get_training_dataset()`, etc.
2. **Training Data Views** — Pre-joined customer/product/sales-rep datasets ready for ML
3. **Documentation** (`FEATURE_STORE_GUIDE.md`) — Architecture, PIT correctness, use cases, examples

### Why Point-in-Time Correctness Matters
When training ML models, **data leakage** occurs if you use information from *after* the prediction date. The feature store preserves time capsules:
```sql
-- Leaky: uses all history
SELECT * FROM customer_features WHERE CUSTOMER_ID = 123;

-- PIT Correct: features as of 2000-01-01 (no future data)
SELECT * FROM customer_rfm_features_offline 
WHERE CUSTOMER_ID = 123 AND OBSERVATION_DATE = '2000-01-01';
```

### Acceptance Gate
1. Feature store schema created: ≥6 tables in `FEATURE_STORE` schema
2. Feature registry populated: ≥20 features cataloged
3. Python API callable and returns correct row counts
4. Point-in-time correctness verified with sample queries
5. Training data views joinable and produce expected row counts

---

## Performance and Cost Guardrails

1. Clustering policy defined per large fact and heavy-scan mart tables.
2. Search optimization applied only to high-value selective lookup patterns.
3. Warehouse sizing matrix documented for XS, S, and M by workload type.
4. Auto-suspend, auto-resume, and caching strategy documented and validated.
5. Benchmark suite shows pre/post optimization latency deltas and credit impact guidance.

---

## Verification and Release Gates

1. Gate A: Source and mapping consistency
   - All scripts execute against approved TPCH baseline mappings.
2. Gate B: Platform capability health
   - Dynamic Table refresh healthy.
   - Stream lag within threshold.
   - Task success rate meets target.
   - Snowpark runs succeed with no critical failures.
3. Gate C: Data quality and trust
   - DAMA 6 metrics logged with pass/fail counts.
   - Rejected-record handling validated.
4. Gate D: Product SLA compliance
   - Each product reports freshness, latency, completeness, and accuracy results.
5. Gate E: Marketplace readiness
   - Listing discoverable and consumer validation flow passes.
6. Gate F: Performance and cost evidence
   - Benchmark results and warehouse-credit guidance documented.

---

## Decisions and Scope Boundaries

| Decision ID | Decision | Status |
|---|---|---|
| D1 | TPC-H is canonical source baseline | Approved |
| D2 | Dynamic Tables, Streams, Tasks, Snowpark are mandatory | Approved |
| D3 | AI-ready is committed as architecture plus stubs | Approved |
| D4 | Custom Marketplace UI is stretch and non-blocking | Approved |

Included scope:
1. Foundation through Marketplace publication.
2. Platform operationalization and SLA-backed products.
3. AI-ready structural scaffolding.

Excluded scope:
1. Production-grade custom marketplace application.
2. Full RAG application and model-serving platform.
3. External IaC and enterprise deployment automation.

Escalation rule:
1. Any requirement that changes D1-D4 triggers plan revision before implementation.

---

## Requirements Traceability Matrix

| Requirement ID | Requirement | Artifact(s) | Owner | Verification Method | Status |
|---|---|---|---|---|---|
| R1 | Canonical TPCH baseline | `setup_guide.md`, `plan.md` | Data Platform | Source consistency gate | Planned |
| R2 | Mandatory Dynamic Tables | `03_phase2_silver.sql`, `check_status.py` | Data Engineering | Platform capability gate | Planned |
| R3 | Mandatory Streams | `02_phase2_bronze.sql`, `check_status.py` | Data Engineering | Stream lag check | Planned |
| R4 | Mandatory Tasks | `05_phase3_data_products.sql`, `run_all.py` | Data Engineering | Task success rate check | Planned |
| R5 | Mandatory Snowpark | `03_phase2_silver.sql` | Data Engineering | Snowpark run status | Planned |
| R6 | AI-ready architecture stubs | `plan.md`, `data_products.md` | Architecture | AI-ready gate | Planned |
| R7 | SLA-backed products | `data_products.md`, `verify_roles_sla.py` | Product Analytics | SLA compliance gate | Planned |
| R8 | Marketplace publication | `marketplace_listing.txt` | Data Product Owner | Consumer flow test | Planned |
| R9 | Stretch UI non-blocking | `snowtest.md`, `plan.md` | Architecture | Scope boundary review | Planned |
| R10 | ML Feature Store (PIT correctness) | `06_feature_store_ml.sql`, `feature_store.py`, `FEATURE_STORE_GUIDE.md` | Data Science / ML | Feature registry populated; training data tests pass | Planned |