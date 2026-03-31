# Updated Requirements: Snowflake Sales Data Platform

## Committed Scope (Must Deliver)

### Foundation and Source Baseline
- R1: Use `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1` as canonical source baseline.
- R2: Create and use `RAW_SALES` schemas: `LANDING`, `BRONZE`, `SILVER`, `GOLD`, `MONITORING`.
- R3: Publish approved source-to-domain mapping for customers, orders, order items, products, and territory or sales attribution.

### Medallion plus Platform Services
- R4: Implement Medallion architecture with explicit layer responsibilities.
- R5: Implement Dynamic Tables as required incremental transformation capability.
- R6: Implement Streams as required change-capture capability.
- R7: Implement Tasks as required orchestration capability.
- R8: Implement Snowpark for complex transformation or validation logic.

### Data Quality and Trust
- R9: Apply DAMA 6 quality dimensions in Silver with measurable pass and fail outputs.
- R10: Persist rejected records with rejection reasons for auditability.
- R11: Persist monitoring outputs for freshness, completeness, validity, and drift indicators.

### Business Serving and Performance
- R12: Deliver Gold dimensional model and BI-ready aggregate marts.
- R13: Apply clustering and search optimization based on query patterns.
- R14: Document warehouse sizing and cost guardrails for XS, S, and M workloads.

### Data Products and Governance
- R15: Deliver three contract-based data products with explicit input and output definitions.
- R16: Enforce SLA dimensions for each product: freshness, latency, completeness, accuracy, availability.
- R17: Enforce role-based access controls plus ownership and escalation metadata.

### Marketplace Delivery
- R18: Publish Snowflake Marketplace listing for curated data products.
- R19: Validate consumer journey: discover, subscribe, and query sample use cases.

### AI-Ready Architecture (Committed as Stubs)
- R20: Deliver AI-ready architectural scaffolding with semantic metadata model and retrieval-path stubs.
- R21: Deliver embedding-ready schema fields and queryable metadata artifacts.
- R22: Document RAG-oriented retrieval flow assumptions without full application implementation.

## Stretch Scope (Non-Blocking)
- S1: Build custom marketplace discovery and access portal.
- S2: Deploy stretch UI on AWS free-tier compatible setup.
- S3: Add AI-powered metadata search in custom UI layer.
- Stretch scope must not block committed release acceptance.

## Explicit Non-Goals (Current Release)
- N1: Full production custom marketplace application with enterprise SLOs.
- N2: Full RAG application productization and model-serving platform.
- N3: Enterprise-scale IaC rollout across all resources.
- N4: Organization-wide rollout beyond pilot scope.

## Acceptance Criteria by Category

### Source and Foundation
- A1: All phase scripts execute against TPCH baseline mappings with no undocumented assumptions.

### Platform Capabilities
- A2: Dynamic Table refresh health is valid.
- A3: Stream lag is within threshold.
- A4: Task success rate meets target.
- A5: Snowpark runs have no critical failures.

### Data Quality
- A6: DAMA 6 checks produce persisted metrics and pass and fail counts.
- A7: Rejected-record pathway validated with at least one failure scenario.

### Product SLAs
- A8: Each product reports freshness, latency, completeness, and accuracy evidence against targets.

### Marketplace
- A9: Consumer test account can discover, subscribe, and query documented sample scenarios.

### Performance and Cost
- A10: Benchmark results show pre and post optimization latency deltas.
- A11: Warehouse guidance includes credit-aware recommendations by workload class.

## Requirement Decision Log
- D1: Canonical source baseline is TPC-H in Snowflake. Status: Approved.
- D2: Dynamic Tables, Streams, Tasks, and Snowpark are mandatory. Status: Approved.
- D3: AI-ready scope is architecture plus stubs, not full app. Status: Approved.
- D4: Custom Marketplace UI remains stretch and non-blocking. Status: Approved.

## Change Control Trigger
Any proposed change impacting D1 through D4 requires:
1. Requirement update in `snowtest.md`.
2. Plan update in `plan.md`.
3. Verification update in SQL or Python validation assets.