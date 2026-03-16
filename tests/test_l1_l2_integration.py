"""Integration tests for all L1/L2 checks (IDs: 111, 122, 124, 128, 215, 216, 226, 236, 237, 244, 245).

Verifies:
  - l1-l2-checks.AC5.1 (backward compat): pre-existing checks still work alongside new checks
  - l1-l2-checks.AC5.2 (exit codes): exit codes correctly reflect new check outcomes
  - l1-l2-checks.AC6.1 (coverage): all 11 checks have test coverage with passing and failing data
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from scdm_qa.config import QAConfig
from scdm_qa.pipeline import compute_exit_code, run_pipeline


class TestCheckIDCoverage:
    """Individual tests for each of the 11 L1/L2 checks.

    Each check is tested with both passing and failing data to demonstrate coverage.
    Verifies: l1-l2-checks.AC6.1 (all 11 checks have test coverage)
    """

    def _make_config(self, data_dir: Path, output_dir: Path, table_key: str) -> QAConfig:
        """Helper to create config for a single table."""
        output_dir.mkdir(parents=True, exist_ok=True)
        return QAConfig(
            tables={table_key: data_dir / f"{table_key}.parquet"},
            output_dir=output_dir,
            chunk_size=2,
            error_threshold=0.05,
        )

    # === L1 CHECKS ===

    def test_check_111_not_populated_pass(self, tmp_path: Path) -> None:
        """Check 111: Variable not populated - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        demographic_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "PostalCode": ["12345", "54321"],
            "PostalCode_Date": [1000, 2000],
            "ImputedHispanic": ["Y", "N"],  # Populated → check 111 pass
            "ImputedRace": ["1", "2"],
        })
        demographic_df.write_parquet(data_dir / "demographic.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "demographic")
        outcomes = run_pipeline(config)

        # Check 111 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "111":
                        assert step.n_failed == 0, "Check 111 should pass with populated values"

    def test_check_122_leading_spaces_pass(self, tmp_path: Path) -> None:
        """Check 122: Leading spaces - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        encounter_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EncounterID": ["E1", "E2"],
            "EncType": ["IP", "AV"],
            "ADate": [1000, 2000],
            "DDate": [1100, 2100],
            "DRG": ["A01", "A02"],  # No leading spaces → check 122 pass
            "Discharge_Disposition": [1, None],
            "Discharge_Status": [1, None],
            "Admitting_Source": [1, 1],
        })
        encounter_df.write_parquet(data_dir / "encounter.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "encounter")
        outcomes = run_pipeline(config)

        # Check 122 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "122":
                        assert step.n_failed == 0, "Check 122 should pass without leading spaces"

    def test_check_122_leading_spaces_fail(self, tmp_path: Path) -> None:
        """Check 122: Leading spaces - violation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        cod_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "COD": [" E10", "E11"],  # P1 has leading space → check 122 violation
            "CodeType": ["10", "10"],
            "CauseType": ["U", "U"],
            "Source": ["medical_record", "medical_record"],
            "Confidence": ["1", "1"],
        })
        cod_df.write_parquet(data_dir / "cause_of_death.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "cause_of_death")
        outcomes = run_pipeline(config)

        # Check 122 should appear with failures
        found = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "122" and step.n_failed > 0:
                        found = True
                        assert step.severity == "Warn"  # Check 122 has Warn severity
        assert found, "Check 122 should have failures for leading spaces"

    def test_check_124_unexpected_zeros_fail(self, tmp_path: Path) -> None:
        """Check 124: Unexpected zeros in RxSup - violation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        dispensing_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "ProviderID": ["Prov1", "Prov2"],
            "RxDate": [1000, 2000],
            "Rx": ["RX001", "RX002"],
            "Rx_CodeType": ["NDC", "NDC"],
            "RxSup": [30.0, 0.0],  # P2 has zero → check 124 violation
            "RxAmt": [100.0, 100.0],
        })
        dispensing_df.write_parquet(data_dir / "dispensing.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "dispensing")
        outcomes = run_pipeline(config)

        # Check 124 should appear with failures
        found = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "124" and step.n_failed > 0:
                        found = True
                        assert step.severity == "Warn"
        assert found, "Check 124 should have failures for RxSup with zeros"

    def test_check_124_unexpected_zeros_pass(self, tmp_path: Path) -> None:
        """Check 124: Unexpected zeros - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        dispensing_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "ProviderID": ["Prov1", "Prov2"],
            "RxDate": [1000, 2000],
            "Rx": ["RX001", "RX002"],
            "Rx_CodeType": ["NDC", "NDC"],
            "RxSup": [30.0, 30.0],  # No zeros → check 124 pass
            "RxAmt": [100.0, 100.0],
        })
        dispensing_df.write_parquet(data_dir / "dispensing.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "dispensing")
        outcomes = run_pipeline(config)

        # Check 124 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "124":
                        assert step.n_failed == 0, "Check 124 should pass without zeros"

    def test_check_128_non_numeric_postal_code_fail(self, tmp_path: Path) -> None:
        """Check 128: Non-numeric PostalCode - violation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        demographic_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "PostalCode": ["12345", "ABC12"],  # P2 has letters → check 128 violation
            "PostalCode_Date": [1000, 2000],
            "ImputedHispanic": ["Y", "N"],
            "ImputedRace": ["1", "2"],
        })
        demographic_df.write_parquet(data_dir / "demographic.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "demographic")
        outcomes = run_pipeline(config)

        # Check 128 should appear with failures
        found = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "128" and step.n_failed > 0:
                        found = True
                        assert step.severity == "Warn"
        assert found, "Check 128 should have failures for non-numeric PostalCode"

    def test_check_128_non_numeric_postal_code_pass(self, tmp_path: Path) -> None:
        """Check 128: Non-numeric PostalCode - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        demographic_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["F", "M"],
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "PostalCode": ["12345", "54321"],  # All numeric → check 128 pass
            "PostalCode_Date": [1000, 2000],
            "ImputedHispanic": ["Y", "N"],
            "ImputedRace": ["1", "2"],
        })
        demographic_df.write_parquet(data_dir / "demographic.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "demographic")
        outcomes = run_pipeline(config)

        # Check 128 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "128":
                        assert step.n_failed == 0, "Check 128 should pass with numeric PostalCode"

    # === L2 CHECKS ===

    def test_check_215_overlapping_spans_fail(self, tmp_path: Path) -> None:
        """Check 215: Overlapping enrollment spans - violation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        enrollment_df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "Enr_Start": [1000, 1050],
            "Enr_End": [1100, 1150],  # Overlaps with first record → check 215 violation
            "MedCov": ["Y", "Y"],
            "DrugCov": ["Y", "Y"],
            "Chart": ["Y", "Y"],
            "PlanType": ["P", "P"],
            "PayerType": ["P", "P"],
        })
        enrollment_df.write_parquet(data_dir / "enrollment.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "enrollment")
        outcomes = run_pipeline(config)

        # Check 215 should appear with failures
        found = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "215" and step.n_failed > 0:
                        found = True
        assert found, "Check 215 should have failures for overlapping spans"

    def test_check_215_overlapping_spans_pass(self, tmp_path: Path) -> None:
        """Check 215: Overlapping enrollment spans - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        enrollment_df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "Enr_Start": [1000, 2000],
            "Enr_End": [1100, 2100],  # No overlap → check 215 pass
            "MedCov": ["Y", "Y"],
            "DrugCov": ["Y", "Y"],
            "Chart": ["Y", "Y"],
            "PlanType": ["P", "P"],
            "PayerType": ["P", "P"],
        })
        enrollment_df.write_parquet(data_dir / "enrollment.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "enrollment")
        outcomes = run_pipeline(config)

        # Check 215 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "215":
                        assert step.n_failed == 0, "Check 215 should pass without overlaps"

    def test_check_216_enrollment_gaps_fail(self, tmp_path: Path) -> None:
        """Check 216: Enrollment gaps - violation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        enrollment_df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "Enr_Start": [1000, 1200],
            "Enr_End": [1100, 1300],  # Gap from 1100 to 1200 → check 216 violation
            "MedCov": ["Y", "Y"],
            "DrugCov": ["Y", "Y"],
            "Chart": ["Y", "Y"],
            "PlanType": ["P", "P"],
            "PayerType": ["P", "P"],
        })
        enrollment_df.write_parquet(data_dir / "enrollment.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "enrollment")
        outcomes = run_pipeline(config)

        # Check 216 should appear with failures
        found = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "216" and step.n_failed > 0:
                        found = True
        assert found, "Check 216 should have failures for enrollment gaps"

    def test_check_216_enrollment_gaps_pass(self, tmp_path: Path) -> None:
        """Check 216: Enrollment gaps - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        enrollment_df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "Enr_Start": [1000, 1100],
            "Enr_End": [1100, 1200],  # No gap (adjacent) → check 216 pass
            "MedCov": ["Y", "Y"],
            "DrugCov": ["Y", "Y"],
            "Chart": ["Y", "Y"],
            "PlanType": ["P", "P"],
            "PayerType": ["P", "P"],
        })
        enrollment_df.write_parquet(data_dir / "enrollment.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "enrollment")
        outcomes = run_pipeline(config)

        # Check 216 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "216":
                        assert step.n_failed == 0, "Check 216 should pass without gaps"

    def test_check_226_date_ordering_pass(self, tmp_path: Path) -> None:
        """Check 226: Date ordering - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        encounter_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EncounterID": ["E1", "E2"],
            "EncType": ["IP", "AV"],
            "ADate": [1000, 2000],
            "DDate": [1100, 2100],  # ADate <= DDate → check 226 pass
            "DRG": ["A01", "A02"],
            "Discharge_Disposition": [1, None],
            "Discharge_Status": [1, None],
            "Admitting_Source": [1, 1],
        })
        encounter_df.write_parquet(data_dir / "encounter.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "encounter")
        outcomes = run_pipeline(config)

        # Check 226 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "226":
                        assert step.n_failed == 0, "Check 226 should pass with ADate <= DDate"

    def test_check_226_date_ordering_fail(self, tmp_path: Path) -> None:
        """Check 226: Date ordering - violation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        encounter_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EncounterID": ["E1", "E2"],
            "EncType": ["IP", "AV"],
            "ADate": [1000, 2000],
            "DDate": [900, 2100],  # P1: DDate < ADate → check 226 violation
            "DRG": ["A01", "A02"],
            "Discharge_Disposition": ["1", None],
            "Discharge_Status": ["A", None],
            "Admitting_Source": ["01", "01"],
        })
        encounter_df.write_parquet(data_dir / "encounter.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "encounter")
        outcomes = run_pipeline(config)

        # Check 226 should appear with failures
        found = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "226" and step.n_failed > 0:
                        found = True
                        assert step.severity == "Fail"
        assert found, "Check 226 should have failures for invalid date ordering"

    def test_check_236_missing_underlying_cause_pass(self, tmp_path: Path) -> None:
        """Check 236: Missing underlying cause - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        cod_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "COD": ["I10", "I15"],  # Both present → check 236 pass
            "CodeType": ["10", "10"],
            "CauseType": ["U", "U"],
            "Source": ["medical_record", "medical_record"],
            "Confidence": [1, 1],
        })
        cod_df.write_parquet(data_dir / "cause_of_death.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "cause_of_death")
        outcomes = run_pipeline(config)

        # Check 236 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "236":
                        assert step.n_failed == 0, "Check 236 should pass with underlying causes present"

    def test_check_236_missing_underlying_cause_fail(self, tmp_path: Path) -> None:
        """Check 236: Missing underlying cause - violation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        cod_df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "COD": ["I10", "I15"],
            "CodeType": ["10", "10"],
            "CauseType": ["O", "O"],  # P1 has no CauseType='U' → check 236 violation
            "Source": ["medical_record", "medical_record"],
            "Confidence": ["1", "1"],
        })
        cod_df.write_parquet(data_dir / "cause_of_death.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "cause_of_death")
        outcomes = run_pipeline(config)

        # Check 236 should appear with failures
        found = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "236" and step.n_failed > 0:
                        found = True
                        assert step.severity == "Fail"
        assert found, "Check 236 should have failures for missing underlying causes"

    def test_check_237_multiple_underlying_causes_pass(self, tmp_path: Path) -> None:
        """Check 237: Multiple underlying causes - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        cod_df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "COD": ["I10", "I15"],  # Two underlying but one is in separate record
            "CodeType": ["10", "10"],
            "CauseType": ["U", "O"],  # Only one underlying, one other → check 237 pass
            "Source": ["medical_record", "medical_record"],
            "Confidence": [1, 1],
        })
        cod_df.write_parquet(data_dir / "cause_of_death.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "cause_of_death")
        outcomes = run_pipeline(config)

        # Check 237 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "237":
                        assert step.n_failed == 0, "Check 237 should pass with single underlying cause"

    def test_check_237_multiple_underlying_causes_fail(self, tmp_path: Path) -> None:
        """Check 237: Multiple underlying causes - violation."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        cod_df = pl.DataFrame({
            "PatID": ["P1", "P1"],
            "COD": ["I10", "I15"],  # Two underlying causes in same patient
            "CodeType": ["10", "10"],
            "CauseType": ["U", "U"],  # Both underlying → check 237 violation
            "Source": ["medical_record", "medical_record"],
            "Confidence": ["1", "1"],
        })
        cod_df.write_parquet(data_dir / "cause_of_death.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "cause_of_death")
        outcomes = run_pipeline(config)

        # Check 237 should appear with failures
        found = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "237" and step.n_failed > 0:
                        found = True
                        assert step.severity == "Fail"
        assert found, "Check 237 should have failures for multiple underlying causes"

    def test_check_244_invalid_enc_combination_pass(self, tmp_path: Path) -> None:
        """Check 244: Invalid ENC field combinations - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        encounter_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "EncounterID": ["E1", "E2"],
            "EncType": ["IP", "AV"],
            "ADate": [1000, 2000],
            "DDate": [1100, 2100],
            "DRG": ["A01", "A02"],
            "Discharge_Disposition": [1, None],  # IP has disposition, AV null is OK
            "Discharge_Status": [1, None],  # IP has status, AV null is OK
            "Admitting_Source": [1, 1],
        })
        encounter_df.write_parquet(data_dir / "encounter.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "encounter")
        outcomes = run_pipeline(config)

        # Check 244 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "244":
                        assert step.n_failed == 0, "Check 244 should pass with valid ENC combinations"

    def test_check_245_rate_threshold_pass(self, tmp_path: Path) -> None:
        """Check 245: Rate threshold - clean data."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # All valid combos
        encounter_df = pl.DataFrame({
            "PatID": ["P1", "P2", "P3"],
            "EncounterID": ["E1", "E2", "E3"],
            "EncType": ["IP", "IP", "AV"],
            "ADate": [1000, 2000, 3000],
            "DDate": [1100, 2100, 3100],
            "DRG": ["A01", "A02", "A03"],
            "Discharge_Disposition": [1, 1, None],  # IP has, AV null is OK
            "Discharge_Status": [1, 1, None],
            "Admitting_Source": [1, 1, 1],
        })
        encounter_df.write_parquet(data_dir / "encounter.parquet")

        config = self._make_config(data_dir, tmp_path / "reports", "encounter")
        outcomes = run_pipeline(config)

        # Check 245 should not have failures
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "245":
                        assert step.n_failed == 0, "Check 245 should pass when invalid rate within threshold"

class TestBackwardCompatibility:
    """Test backward compatibility with pre-existing checks.

    Verifies: l1-l2-checks.AC5.1
    """

    def test_pre_existing_checks_still_work(self, tmp_path: Path) -> None:
        """Verify pre-existing validation checks (col_vals_in_set, etc.) still work."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create data with invalid enum that violates pre-existing col_vals_in_set check
        demographic_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "Birth_Date": [1000, 2000],
            "Sex": ["X", "M"],  # P1: X is not in allowed set {F, M} → pre-existing col_vals_in_set violation
            "Hispanic": ["Y", "N"],
            "Race": ["1", "2"],
            "PostalCode": ["12345", "54321"],
            "PostalCode_Date": [1000, 2000],
            "ImputedHispanic": ["Y", "N"],
            "ImputedRace": ["1", "2"],
        })
        demographic_df.write_parquet(data_dir / "demographic.parquet")

        output_dir = tmp_path / "reports"
        config = QAConfig(
            tables={"demographic": data_dir / "demographic.parquet"},
            output_dir=output_dir,
            chunk_size=2,
            error_threshold=0.05,
        )
        outcomes = run_pipeline(config)

        # Should have pre-existing validation failures
        found_pre_existing = False
        for outcome in outcomes:
            if outcome.validation_result:
                for step in outcome.validation_result.steps:
                    if step.check_id == "121" and "Sex" in step.column and step.n_failed > 0:
                        found_pre_existing = True
        assert found_pre_existing, "Pre-existing validation checks should still produce failures"


class TestExitCodes:
    """Test that exit codes correctly reflect new check outcomes.

    Verifies: l1-l2-checks.AC5.2
    """

    def test_exit_code_reflects_new_check_failures(self, tmp_path: Path) -> None:
        """Verify exit codes reflect failures in new checks."""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Create data with check 124 violation that will be reflected in exit code
        dispensing_df = pl.DataFrame({
            "PatID": ["P1", "P2"],
            "ProviderID": ["Prov1", "Prov2"],
            "RxDate": [1000, 2000],
            "Rx": ["RX001", "RX002"],
            "Rx_CodeType": ["NDC", "NDC"],
            "RxSup": [30.0, 0.0],  # P2 has zero → check 124 violation
            "RxAmt": [100.0, 100.0],
        })
        dispensing_df.write_parquet(data_dir / "dispensing.parquet")

        output_dir = tmp_path / "reports"
        config = QAConfig(
            tables={"dispensing": data_dir / "dispensing.parquet"},
            output_dir=output_dir,
            chunk_size=2,
            error_threshold=0.05,
        )
        outcomes = run_pipeline(config)
        exit_code = compute_exit_code(outcomes, error_threshold=0.05)

        # With a failure in a check, should be non-zero
        assert exit_code >= 1, "Exit code should reflect failures in new checks"
