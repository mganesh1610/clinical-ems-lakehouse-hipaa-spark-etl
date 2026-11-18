-- =============================================================================
-- Snowflake & Databricks Unity Catalog RBAC, Dynamic Data Masking & Semantic Marts
-- Separates HIPAA Clinical Cohort Metrics, PII Tokens, and Financial Claim Ledgers
-- =============================================================================

-- 1. Snowflake Dynamic Data Masking Policy for Facility Contact / Address PII
CREATE OR REPLACE MASKING POLICY governance.mask_contact_pii AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('ROLE_COMPLIANCE_AUDITOR', 'SYSADMIN') THEN val
        ELSE CONCAT('TOK-', UPPER(LEFT(SHA2(CONCAT('lakehouse-hipaa-safe-harbor-v1:', COALESCE(val, '')), 256), 12)))
    END;

-- 2. Snowflake Row Access Policy Enforcing CMS Small-Cell Suppression (n >= 11) for Clinical Analysts
CREATE OR REPLACE ROW ACCESS POLICY governance.hipaa_small_cell_rap AS (small_cell_suppressed INTEGER) RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() IN ('ROLE_COMPLIANCE_AUDITOR') THEN TRUE
        WHEN CURRENT_ROLE() IN ('ROLE_CLINICAL_OPS', 'ROLE_EXEC_ANALYTICS') AND small_cell_suppressed = 0 THEN TRUE
        ELSE FALSE
    END;

-- 3. Semantic Star-Schema Analytical CTE Query: Emergency Boarding Root-Cause vs Episode Cost
WITH ed_facility_metrics AS (
    SELECT
        e.facility_id,
        MAX(e.ed_volume_cohort) AS ed_volume_cohort,
        AVG(CASE WHEN e.measure_id = 'OP_18b' THEN e.numeric_score END) AS op18b_arrival_to_departure_min,
        AVG(CASE WHEN e.measure_id = 'OP_22' THEN e.numeric_score END) AS op22_lwbs_pct,
        AVG(CASE WHEN e.measure_id = 'SEP_1' THEN e.numeric_score END) AS sep1_sepsis_compliance_pct
    FROM silver.fct_ed_timely_care e
    WHERE e.small_cell_suppressed = 0
    GROUP BY e.facility_id
),
financial_episode_rollup AS (
    SELECT
        c.facility_id,
        AVG(CASE WHEN c.claim_type = 'Total' THEN c.avg_spending_hospital END) AS total_episode_spending_usd,
        AVG(CASE WHEN c.claim_type = 'Carrier' THEN c.avg_spending_hospital END) AS carrier_transport_spending_usd,
        AVG(CASE WHEN c.claim_type = 'Total' THEN c.hospital_to_national_ratio END) AS spending_to_national_ratio
    FROM silver.fct_medicare_claim_spending c
    GROUP BY c.facility_id
)
SELECT
    f.state,
    f.hospital_type,
    f.dispatch_zone,
    e.ed_volume_cohort,
    COUNT(DISTINCT f.facility_id) AS facility_count,
    ROUND(AVG(e.op18b_arrival_to_departure_min), 2) AS avg_ed_dwell_minutes,
    ROUND(AVG(e.op22_lwbs_pct), 2) AS avg_lwbs_pct,
    ROUND(AVG(e.sep1_sepsis_compliance_pct), 2) AS avg_sepsis_bundle_pct,
    ROUND(AVG(r.total_episode_spending_usd), 2) AS avg_total_episode_usd,
    ROUND(AVG(r.spending_to_national_ratio), 4) AS avg_national_cost_index
FROM silver.dim_facility f
INNER JOIN ed_facility_metrics e ON f.facility_id = e.facility_id
LEFT JOIN financial_episode_rollup r ON f.facility_id = r.facility_id
GROUP BY f.state, f.hospital_type, f.dispatch_zone, e.ed_volume_cohort
ORDER BY facility_count DESC;
