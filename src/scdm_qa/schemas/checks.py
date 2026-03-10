from __future__ import annotations

from scdm_qa.schemas.models import L1CheckDef, DateOrderingDef

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


# Check 226: Date ordering violations
# Source: SAS lkp_all_l2 where CheckID=226
DATE_ORDERING_DEFS: tuple[DateOrderingDef, ...] = (
    DateOrderingDef("226", "encounter", "ADate", "DDate", "Fail", "ADate <= DDate"),
    DateOrderingDef("226", "enrollment", "Enr_Start", "Enr_End", "Fail", "Enr_Start <= Enr_End"),
)


def get_date_ordering_checks_for_table(table_key: str) -> tuple[DateOrderingDef, ...]:
    """Return date ordering check definitions for a given table."""
    return tuple(d for d in DATE_ORDERING_DEFS if d.table_key == table_key)


# Check 244/245: Valid ENC field combinations
# Source: SAS lkp_enc_l2 (96 rows)
#
# The valid combination rules are defined per EncType:
# - IP (Inpatient): DDate Present required, Discharge_Disposition required, Discharge_Status required
# - IS (Institutional Stay): DDate Present required, Discharge_Disposition required, Discharge_Status required
# - ED (Emergency Department): DDate Present or Null, Discharge_Disposition optional, Discharge_Status optional
# - AV (Ambulatory Visit): DDate Null expected, Discharge_Disposition Null expected, Discharge_Status Null expected
# - OA (Other Ambulatory): DDate Null expected, Discharge_Disposition Null expected, Discharge_Status Null expected

# Simplified rules (derived from SAS lkp_enc_l2 analysis):
# Each tuple: (EncType, ddate_required, discharge_disposition_required, discharge_status_required)
ENC_COMBINATION_RULES: dict[str, tuple[bool, bool, bool]] = {
    "IP": (True, True, True),     # DDate, Disposition, Status all required
    "IS": (True, True, True),     # DDate, Disposition, Status all required
    "ED": (False, False, False),  # All optional
    "AV": (False, False, False),  # All optional (DDate/disposition/status should be null)
    "OA": (False, False, False),  # All optional
}

# Check 245 rate thresholds per EncType
# Source: SAS lkp_rate_threshold
# Each EncType has a threshold for what % of invalid combos triggers a flag.
# Defaults below are conservative estimates pending SAS reference confirmation.
ENC_RATE_THRESHOLDS: dict[str, float] = {
    "IP": 0.05,
    "IS": 0.05,
    "ED": 0.10,
    "AV": 0.10,
    "OA": 0.10,
}


def is_valid_enc_combination(
    enc_type: str,
    ddate_state: str,
    discharge_disposition: str | None,
    discharge_status: str | None,
) -> bool:
    """Check if an ENC row matches valid combination rules.

    Returns True if the combination is valid.
    """
    rules = ENC_COMBINATION_RULES.get(enc_type)
    if rules is None:
        return False  # Unknown EncType

    ddate_required, disp_required, status_required = rules

    if ddate_required and ddate_state == "Null":
        return False
    if disp_required and discharge_disposition is None:
        return False
    if status_required and discharge_status is None:
        return False

    return True
