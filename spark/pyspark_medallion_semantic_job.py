"""
Distributed Apache Spark (PySpark & Spark SQL) Bronze -> Silver -> Gold Medallion Job
for Emergency Care, Clinical Timeliness & Medicare Episode Spending Lakehouse.

Designed for Databricks Runtime 15.x LTS, Snowflake Snowpark / Iceberg, and Microsoft Fabric Spark.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


RESTRICTED_SAFE_HARBOR_ZIP3 = [
    "036", "059", "063", "102", "203", "556", "692", "790",
    "821", "823", "830", "831", "878", "879", "884", "890", "893",
]


def build_spark_session(app_name: str = "EmergencyCareMedallionLakehouse") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def normalize_ccn_column(col_expr: F.Column) -> F.Column:
    cleaned = F.regexp_replace(F.trim(col_expr.cast("string")), r"\.0$", "")
    return F.when(cleaned.rlike(r"^\d{1,5}$"), F.lpad(cleaned, 6, "0")).otherwise(F.upper(cleaned))


def transform_silver_facility(df_bronze_hosp: DataFrame, salt: str) -> DataFrame:
    return (
        df_bronze_hosp.withColumn("facility_id", normalize_ccn_column(F.col("Facility ID")))
        .filter(F.length("facility_id") > 0)
        .dropDuplicates(["facility_id"])
        .select(
            "facility_id",
            F.trim(F.col("Facility Name")).alias("facility_name"),
            F.trim(F.col("State")).alias("state"),
            F.trim(F.col("City/Town")).alias("city"),
            F.substring(F.trim(F.col("ZIP Code")), 1, 5).alias("zip5"),
            F.trim(F.col("Hospital Type")).alias("hospital_type"),
            F.trim(F.col("Hospital Ownership")).alias("hospital_ownership"),
            F.when(F.lower(F.trim(F.col("Emergency Services"))) == "yes", F.lit(1)).otherwise(F.lit(0)).alias("emergency_services"),
            F.concat(
                F.lit("TOK-"),
                F.upper(F.substring(F.sha2(F.concat_ws("|", F.lit(salt), F.col("Address"), F.col("ZIP Code")), 256), 1, 12)),
            ).alias("address_pii_token"),
            F.concat(
                F.lit("TOK-"),
                F.upper(F.substring(F.sha2(F.concat_ws(":", F.lit(salt), F.col("Telephone Number")), 256), 1, 12)),
            ).alias("phone_pii_token"),
        )
    )


def register_gold_spark_sql_semantic_views(spark: SparkSession) -> None:
    zip3_list = ",".join(f"'{z}'" for z in RESTRICTED_SAFE_HARBOR_ZIP3)
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW vw_gold_hipaa_clinical_safe_harbor AS
        SELECT
            f.facility_id,
            f.state,
            CASE
                WHEN SUBSTRING(f.zip5, 1, 3) IN ({zip3_list}) THEN '000'
                ELSE SUBSTRING(f.zip5, 1, 3)
            END AS hipaa_safe_harbor_zip3,
            f.hospital_type,
            f.emergency_services,
            e.measure_id,
            e.condition_category,
            e.ed_volume_cohort,
            e.numeric_score,
            e.sample_size
        FROM silver_ed_timely_care e
        INNER JOIN /*+ BROADCAST(f) */ silver_dim_facility f
            ON e.facility_id = f.facility_id
        WHERE e.small_cell_suppressed = 0
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW vw_gold_financial_claim_ledger AS
        SELECT
            c.facility_id,
            f.state,
            f.hospital_type,
            f.hospital_ownership,
            c.episode_period,
            c.claim_type,
            c.avg_spending_hospital,
            c.avg_spending_state,
            c.avg_spending_national,
            ROUND(c.avg_spending_hospital / NULLIF(c.avg_spending_national, 0), 4) AS hospital_to_national_ratio
        FROM silver_medicare_claim_spending c
        INNER JOIN /*+ BROADCAST(f) */ silver_dim_facility f
            ON c.facility_id = f.facility_id
        """
    )
