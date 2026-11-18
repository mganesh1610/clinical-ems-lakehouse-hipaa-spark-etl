# Data Architecture, Schema Dictionary & HIPAA / PII / Financial Access Governance

## 1. Multi-Source Medallion Architecture

1. **Bronze Ingestion Layer (`SQL` + `REST APIs` + `Excel Spreadsheets`):**
   - **CMS Provider Data CSV & SQL Staging (`230,394` rows):** `Hospital_General_Information_2.csv` (`5,435` rows), `Timely_and_Effective_Care-Hospital.csv` (`161,313` rows), and `Medicare_Hospital_Spending_by_Claim.csv` (`63,646` rows) verified by pinned `SHA-256` digests.
   - **CMS Metastore REST APIs (`JSON`):** Live schema, theme, and modification timestamp validation across `xubh-q36u`, `yv7e-xc69`, and `nrth-mfg3`.
   - **Regional Emergency Transfer Spreadsheets (`.xlsx` via `openpyxl`):** Sheet-aware parser automatically skips `README_Instructions` helper tabs, repairs `575` Excel integer-truncated CMS Certification Numbers (`10001` -> `010001`), and removes `18` duplicate regional rows.
2. **Silver Normalized Relational Star Schema:**
   - `dim_facility` (`5,435` unique CCNs): Hospital type, ownership, emergency capability, regional transfer tier, dispatch zone, and salted `SHA-256` contact PII tokens (`TOK-...`).
   - `dim_clinical_measure` (`35` clinical/emergency quality measures): `OP_18b`, `OP_22`, `OP_23`, `OP_40`, `SEP_1`, `EDV`, stroke, VTE, and hospital harm indicators.
   - `fct_ed_timely_care` (`161,313` rows): Emergency throughput minutes, clinical bundle scores, sample denominators, and `small_cell_suppressed` flags (`n < 11`).
   - `fct_medicare_claim_spending` (`62,942` FK-verified rows + `704` unmapped CCN rows isolated in `audit_orphan_quarantine`): Episode spending across 8 Medicare claim categories.
3. **Gold Role-Separated Semantic Views (`HIPAA` vs. `PII` vs. `Financial`):**

| Security Domain | Semantic View | Permitted Roles | Enforced Controls | Excluded Sensitive Domains |
| :--- | :--- | :--- | :--- | :--- |
| **HIPAA Safe Harbor Clinical** | `vw_hipaa_clinical_safe_harbor` | `ROLE_CLINICAL_OPS` | 3-digit `ZIP-3` (`000` on `<20K` pop prefixes) + `n < 11` small-cell suppression (`552` rows masked) | Excludes Medicare claim spending, raw `ZIP-5`, facility street addresses, and phone numbers |
| **Financial Claim Ledger** | `vw_financial_claim_ledger` | `ROLE_FINANCE_REVENUE` | Episode Medicare claim ratios & cost benchmarks | Excludes clinical patient sample denominators, clinical quality cohorts, and contact PII |
| **PII & Lineage Governance** | `vw_pii_governance_audit` | `ROLE_COMPLIANCE_AUDITOR` | Salted `SHA-256` deterministic tokens (`TOK-...`) + orphan CCN quarantine counts | Excludes unhashed raw PII and row-level clinical/financial measures |
| **Executive Semantic Summary** | `vw_executive_semantic_summary` | `ROLE_EXEC_ANALYTICS` | State × Hospital Type aggregated KPIs (`175` summary cohorts) | Zero row-level facility PII or unsuppressed small cells |
