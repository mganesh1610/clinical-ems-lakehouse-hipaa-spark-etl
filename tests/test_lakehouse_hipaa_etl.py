import json
import sqlite3
import unittest
from pathlib import Path

from src.lakehouse_etl_pipeline import (
    DependencyWorkloadDAG,
    ROOT_DIR,
    mask_pii_token,
    normalize_ccn,
    run_pipeline,
)


class TestClinicalEmsLakehouseHipaaETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.report = run_pipeline()
        cls.db_path = ROOT_DIR / ".data_cache" / "clinical_ems_lakehouse.sqlite"

    def test_01_all_sixteen_quality_and_rbac_gates_pass(self) -> None:
        self.assertTrue(self.report["all_gates_passed"])
        self.assertEqual(self.report["gates_passed"], 16)
        self.assertEqual(self.report["gates_total"], 16)

    def test_02_ccn_leading_zero_normalization(self) -> None:
        ccn, was_trunc = normalize_ccn(10001)
        self.assertEqual(ccn, "010001")
        self.assertTrue(was_trunc)
        ccn2, was_trunc2 = normalize_ccn("040016")
        self.assertEqual(ccn2, "040016")
        self.assertFalse(was_trunc2)

    def test_03_pii_sha256_tokenization_deterministic_and_non_reversible(self) -> None:
        raw = "(602) 555-0199"
        tok1 = mask_pii_token(raw)
        tok2 = mask_pii_token(raw)
        self.assertEqual(tok1, tok2)
        self.assertTrue(tok1.startswith("TOK-"))
        self.assertNotIn("555", tok1)

    def test_04_dag_topological_dependency_and_cycle_guard(self) -> None:
        dag = DependencyWorkloadDAG()
        dag.register_task("bronze", [], 10.0, "BRONZE")
        dag.register_task("silver", ["bronze"], 10.0, "SILVER")
        dag.register_task("gold", ["silver"], 10.0, "GOLD")
        self.assertEqual(dag.topological_order(), ["bronze", "silver", "gold"])

        cyclic = DependencyWorkloadDAG()
        cyclic.register_task("a", ["b"], 5.0, "BRONZE")
        cyclic.register_task("b", ["a"], 5.0, "SILVER")
        with self.assertRaises(RuntimeError):
            cyclic.topological_order()

    def test_05_hipaa_financial_pii_view_column_isolation(self) -> None:
        conn = sqlite3.connect(str(self.db_path))
        cur = conn.cursor()
        hipaa_cols = {r[1] for r in cur.execute("PRAGMA table_info(vw_hipaa_clinical_safe_harbor)").fetchall()}
        fin_cols = {r[1] for r in cur.execute("PRAGMA table_info(vw_financial_claim_ledger)").fetchall()}
        self.assertTrue(hipaa_cols.isdisjoint({"avg_spending_hospital", "claim_type", "zip5", "phone_pii_token"}))
        self.assertTrue(fin_cols.isdisjoint({"numeric_score", "sample_size", "measure_id", "phone_pii_token"}))
        conn.close()

    def test_06_validation_report_and_semantic_csv_emitted(self) -> None:
        report_file = ROOT_DIR / "outputs" / "lakehouse_hipaa_validation_report.json"
        csv_file = ROOT_DIR / "outputs" / "executive_semantic_state_hospital_summary.csv"
        self.assertTrue(report_file.exists())
        self.assertTrue(csv_file.exists())
        loaded = json.loads(report_file.read_text(encoding="utf-8"))
        self.assertEqual(loaded["warehouse_and_rbac_metrics"]["source_rows_total"], 230394)


if __name__ == "__main__":
    unittest.main()
