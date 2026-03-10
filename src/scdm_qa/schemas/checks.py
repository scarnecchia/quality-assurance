from __future__ import annotations

from scdm_qa.schemas.models import L1CheckDef

# Check 122: Leading spaces in character fields
# Source: SAS lkp_all_l1 where CheckID=122
_CHECK_122_DEFS: tuple[L1CheckDef, ...] = (
    L1CheckDef("122", "cause_of_death", "COD", "leading_spaces", "Warn"),
    L1CheckDef("122", "encounter", "DRG", "leading_spaces", "Warn"),
    L1CheckDef("122", "inpatient_pharmacy", "NDC", "leading_spaces", "Warn"),
    L1CheckDef("122", "inpatient_pharmacy", "RxRoute", "leading_spaces", "Warn"),
    L1CheckDef("122", "inpatient_pharmacy", "RxUOM", "leading_spaces", "Warn"),
    L1CheckDef("122", "lab_result", "LOINC", "leading_spaces", "Warn"),
    L1CheckDef("122", "lab_result", "MS_Result_unit", "leading_spaces", "Warn"),
    L1CheckDef("122", "lab_result", "Norm_Range_low", "leading_spaces", "Warn"),
    L1CheckDef("122", "lab_result", "Norm_Range_high", "leading_spaces", "Warn"),
    L1CheckDef("122", "tranx", "TransCode", "leading_spaces", "Warn"),
)

# Check 124: Unexpected zeros in numeric fields
# Source: SAS lkp_all_l1 where CheckID=124
_CHECK_124_DEFS: tuple[L1CheckDef, ...] = (
    L1CheckDef("124", "dispensing", "RxSup", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "dispensing", "RxAmt", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "prescribing", "RxSup", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "prescribing", "RxAmt", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "vital_signs", "HT", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "vital_signs", "WT", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "vital_signs", "Diastolic", "unexpected_zeros", "Warn"),
    L1CheckDef("124", "vital_signs", "Systolic", "unexpected_zeros", "Warn"),
)

# Check 128: Non-numeric characters in PostalCode
# Source: SAS lkp_all_l1 where CheckID=128
_CHECK_128_DEFS: tuple[L1CheckDef, ...] = (
    L1CheckDef("128", "demographic", "PostalCode", "non_numeric", "Warn"),
)

# Check 111: Variable not populated (entirely null column)
# Source: SAS lkp_all_flags where CheckID=111
_CHECK_111_DEFS: tuple[L1CheckDef, ...] = (
    L1CheckDef("111", "demographic", "ImputedHispanic", "not_populated", "Note"),
    L1CheckDef("111", "demographic", "ImputedRace", "not_populated", "Note"),
    L1CheckDef("111", "diagnosis", "PDX", "not_populated", "Fail"),
    L1CheckDef("111", "encounter", "DDate", "not_populated", "Fail"),
    L1CheckDef("111", "encounter", "Discharge_Disposition", "not_populated", "Fail"),
    L1CheckDef("111", "encounter", "Discharge_Status", "not_populated", "Fail"),
    L1CheckDef("111", "encounter", "Admitting_Source", "not_populated", "Fail"),
    L1CheckDef("111", "enrollment", "PlanType", "not_populated", "Note"),
    L1CheckDef("111", "enrollment", "PayerType", "not_populated", "Note"),
)

ALL_L1_CHECKS: tuple[L1CheckDef, ...] = (
    *_CHECK_122_DEFS,
    *_CHECK_124_DEFS,
    *_CHECK_128_DEFS,
    *_CHECK_111_DEFS,
)


def get_l1_checks_for_table(table_key: str) -> tuple[L1CheckDef, ...]:
    """Return all L1 check definitions for a given table key."""
    return tuple(c for c in ALL_L1_CHECKS if c.table_key == table_key)


def get_per_chunk_checks_for_table(table_key: str) -> tuple[L1CheckDef, ...]:
    """Return L1 checks that run per-chunk (122, 124, 128) for a given table."""
    return tuple(
        c for c in ALL_L1_CHECKS
        if c.table_key == table_key and c.check_type != "not_populated"
    )


def get_not_populated_checks_for_table(table_key: str) -> tuple[L1CheckDef, ...]:
    """Return L1 check-111 definitions for a given table (global check)."""
    return tuple(
        c for c in ALL_L1_CHECKS
        if c.table_key == table_key and c.check_type == "not_populated"
    )
