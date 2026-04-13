"""
DEBUG SCRIPT: Output factors + adjusted_float_vector for each tranche
=====================================================================
Outputs X_Actual, X_Fixed, X_Increased, X_Decreased, Experience_Factor,
Interpolation_Vector, Adjustment_Factor, and full Adjusted_Float_Vector
for a chosen scenario and offset.

Usage:
    python debug_afv_scenario1_offset1.py

Edit SCENARIO_NUMBER and TARGET_OFFSET in the CONFIG section below.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sensitivity import (
    load_prophet_data,
    get_prophet_claims_for_deal,
    calculate_tranche_multipliers,
    get_offset_date,
    _get_month_end_timestamp,
    _months_difference,
    _prepare_tranche_arrays,
    _compute_adjusted_float_vector_fast,
    _precompute_blending_arrays,
    _get_tranche_inflation_lookup,
    _compute_adjust_to_actual_claim,
    DEAL_PROPHET_MAPPING,
    PROPHET_SHEETS,
    SCENARIOS,
)
from const import (
    RGA_SHARE_BY_DEAL,
    get_deal_name_from_rga_tab,
)
from client_data_extractor import (
    load_fixed_vectors_data,
    extract_client_data,
    extract_fee_vectors,
)
from calculation import (
    load_sonia_rates,
    calculate_all_tranches,
)
from logging_config import setup_logging

# =============================================================================
# CONFIG — edit these to select scenario, offset, and file paths
# =============================================================================
VALUATION_DATE = "2025-09-30"

# Which scenario to run (1–6)
SCENARIO_NUMBER = 1

# Which offset to run (0 = valuation date, 1 = val + 1 month, etc.)
TARGET_OFFSET = 1

# Number of AFV elements to print per tranche (first N values)
PRINT_HEAD = 20

# Output CSV path (set to None to skip CSV export)
OUTPUT_CSV = "data/output/debug_afv_scenario_offset.csv"

# File paths — edit to match your environment
CLIENT_FILE = (
    "G:/Not treaty specific/TRTYREVW/UK Longevity Swap/"
    "Collateral Calculation/2025/9_September 30 2025/Rothesay Calls/"
    "Collateral Supporting Information RGA 01Oct2025_KS.xlsx"
)
FIXED_VECTOR_FILE = (
    "G:/Not treaty specific/TRTYREVW/UK Longevity Swap/"
    "Collateral Calculation/2025/11_November 30 2025/Rothesay Calls/"
    "Nov_Reconcile/Fixed_Vector_Consolidated.xlsx"
)
PROPHET_FILE = (
    "G:/Not treaty specific/TRTYREVW/UK Longevity Swap/"
    "Demographics/2025_Q3/2025Q3/Prophet Output/"
    "20250930 Prophet Query - Longevity Deflated.xlsx"
)
SONIA_FILE = (
    "G:/Not treaty specific/TRTYREVW/UK Longevity Swap/"
    "Collateral Calculation/Monthly Swap Rate/"
    "Monthly Swap Rates Summary_2026.xlsx"
)


# =============================================================================
# Helper: compute factors WITH X values
# =============================================================================
# _compute_factors_vectorized in sensitivity.py does not return X values.
# This function mirrors its logic but also returns X_Actual/Fixed/Inc/Dec.

def compute_factors_with_x_values(tranche_arrays, anchor_month_end,
                                  inception_date, blended_claims):
    """
    Same as _compute_factors_vectorized but also returns X ratio values.
    """
    date_to_idx = tranche_arrays['date_to_idx']
    fixed_w_real = tranche_arrays['fixed_w_real']
    increased_w_real = tranche_arrays['increased_w_real']
    decreased_w_real = tranche_arrays['decreased_w_real']

    # Credibility factor
    months_since_inception = _months_difference(inception_date, anchor_month_end) + 1
    raw_months = max(months_since_inception - 6, 0)
    capped_months = min(raw_months, 60)
    credibility_factor = (capped_months / 60.0) ** 2 if capped_months > 0 else 0.0

    # Lookup helper
    def get_val_at_offset(array, offset):
        target = (anchor_month_end.to_period('M') + offset).to_timestamp('M')
        idx = date_to_idx.get(target)
        return array[idx] if idx is not None else None

    # Values at -66 and -6
    c66_actual = get_val_at_offset(blended_claims, -66)
    c6_actual = get_val_at_offset(blended_claims, -6)
    c66_fixed = get_val_at_offset(fixed_w_real, -66)
    c6_fixed = get_val_at_offset(fixed_w_real, -6)
    c66_increased = get_val_at_offset(increased_w_real, -66)
    c6_increased = get_val_at_offset(increased_w_real, -6)
    c66_decreased = get_val_at_offset(decreased_w_real, -66)
    c6_decreased = get_val_at_offset(decreased_w_real, -6)

    # Fallback for -66 (same as _compute_factors_vectorized)
    if c66_actual is None:
        c66_actual = blended_claims[0] if len(blended_claims) > 0 else 0.0
    if c66_fixed is None:
        c66_fixed = fixed_w_real[0] if len(fixed_w_real) > 0 else 0.0
    if c66_increased is None:
        c66_increased = increased_w_real[0] if len(increased_w_real) > 0 else 0.0
    if c66_decreased is None:
        c66_decreased = decreased_w_real[0] if len(decreased_w_real) > 0 else 0.0

    # X ratios
    def safe_ratio(num, den):
        if num is None or den is None or den == 0:
            return None
        if np.isnan(num) or np.isnan(den):
            return None
        return float(num) / float(den)

    num_actual = (c66_actual - c6_actual) if c66_actual is not None and c6_actual is not None else None
    x_actual = safe_ratio(num_actual, c66_actual)
    num_fixed = (c66_fixed - c6_fixed) if c66_fixed is not None and c6_fixed is not None else None
    x_fixed = safe_ratio(num_fixed, c66_fixed)
    num_inc = (c66_increased - c6_increased) if c66_increased is not None and c6_increased is not None else None
    x_increased = safe_ratio(num_inc, c66_increased)
    num_dec = (c66_decreased - c6_decreased) if c66_decreased is not None and c6_decreased is not None else None
    x_decreased = safe_ratio(num_dec, c66_decreased)

    # Experience raw
    experience_raw = 0.0
    if all(v is not None for v in [x_actual, x_fixed, x_increased, x_decreased]):
        if abs(x_actual - x_fixed) < 1e-6:
            experience_raw = 0.0
        elif x_fixed < x_actual:
            denom = x_fixed - x_decreased
            experience_raw = 0.0 if abs(denom) < 1e-6 else (x_fixed - x_actual) / denom
        else:
            denom = x_increased - x_fixed
            experience_raw = 0.0 if abs(denom) < 1e-6 else (x_actual - x_fixed) / denom

    experience_factor = experience_raw * credibility_factor

    # Interpolation vector
    if x_fixed is None or x_actual is None:
        interpolation_vector = 0
    else:
        interpolation_vector = 0 if x_fixed < x_actual else 1

    # Adjustment factor from sums at -8, -7, -6
    sum_claims = 0.0
    sum_fixed = 0.0
    sum_increased = 0.0
    sum_decreased = 0.0

    for off in [-8, -7, -6]:
        target = (anchor_month_end.to_period('M') + off).to_timestamp('M')
        idx = date_to_idx.get(target)
        if idx is not None:
            sum_claims += blended_claims[idx] if not np.isnan(blended_claims[idx]) else 0.0
            sum_fixed += fixed_w_real[idx] if not np.isnan(fixed_w_real[idx]) else 0.0
            sum_increased += increased_w_real[idx] if not np.isnan(increased_w_real[idx]) else 0.0
            sum_decreased += decreased_w_real[idx] if not np.isnan(decreased_w_real[idx]) else 0.0

    if sum_claims == 0 and sum_fixed == 0 and sum_increased == 0 and sum_decreased == 0:
        adjustment_factor = 1.0
    else:
        base_denom = (1.0 - experience_factor) * sum_fixed
        if interpolation_vector == 0:
            base_denom += experience_factor * sum_decreased
        else:
            base_denom += experience_factor * sum_increased
        adjustment_factor = sum_claims / base_denom if base_denom != 0 else 1.0

    return {
        'X_Actual': x_actual,
        'X_Fixed': x_fixed,
        'X_Increased': x_increased,
        'X_Decreased': x_decreased,
        'Experience_Factor': experience_factor,
        'Interpolation_Vector': interpolation_vector,
        'Adjustment_Factor': adjustment_factor,
        'Credibility_Factor': credibility_factor,
        # Extra intermediates for deep debugging
        'c66_actual': c66_actual, 'c6_actual': c6_actual,
        'c66_fixed': c66_fixed, 'c6_fixed': c6_fixed,
        'c66_increased': c66_increased, 'c6_increased': c6_increased,
        'c66_decreased': c66_decreased, 'c6_decreased': c6_decreased,
        'experience_raw': experience_raw,
        'sum_claims_adj': sum_claims,
        'sum_fixed_adj': sum_fixed,
        'sum_increased_adj': sum_increased,
        'sum_decreased_adj': sum_decreased,
    }


def main():
    setup_logging(log_dir="data/output/logs")

    # ------------------------------------------------------------------
    # Resolve scenario config from SCENARIO_NUMBER
    # ------------------------------------------------------------------
    scenario = SCENARIOS[SCENARIO_NUMBER - 1]  # 1-indexed
    scenario_name = scenario["name"]
    grading_period = scenario["grading_period"]
    discount_shock = scenario["discount_rate_shock"]
    claims_type = scenario["claims_type"]

    # Blend factor for this offset
    if grading_period == 0:
        blend_factor = 0.0
    else:
        blend_factor = min(1.0, TARGET_OFFSET / grading_period)

    print("=" * 80)
    print(f"DEBUG: factors + adjusted_float_vector")
    print(f"  Scenario {SCENARIO_NUMBER}: {scenario_name}  "
          f"(claims={claims_type}, gp={grading_period}, shock={discount_shock})")
    print(f"  Offset   : {TARGET_OFFSET}")
    print(f"  Blend    : {blend_factor:.10f}")
    print("=" * 80)
    print()

    # ------------------------------------------------------------------
    # 1. Load input data (mirrors main.py Steps 1–4)
    # ------------------------------------------------------------------
    print("[1/7] Loading fixed vectors...")
    fixed_vectors_data = load_fixed_vectors_data(FIXED_VECTOR_FILE)

    print("[2/7] Extracting client data...")
    client_claims_data, client_vectors_data = extract_client_data(CLIENT_FILE)

    print("[3/7] Extracting fee vectors...")
    fee_vectors_data = extract_fee_vectors(CLIENT_FILE)

    print("[4/7] Calculating all tranches (comprehensive_results)...")
    comprehensive_results, _ = calculate_all_tranches(
        fixed_vectors_data, client_claims_data, client_vectors_data,
        fee_vectors_data, VALUATION_DATE
    )
    print(f"       -> {len(comprehensive_results)} tranches processed")

    # ------------------------------------------------------------------
    # 2. Load Prophet data
    # ------------------------------------------------------------------
    print("[5/7] Loading Prophet data (BE + 1in200)...")
    prophet_be_df = load_prophet_data(PROPHET_FILE, PROPHET_SHEETS["BE"])
    prophet_stress_df = load_prophet_data(PROPHET_FILE, PROPHET_SHEETS["1in200"])

    # ------------------------------------------------------------------
    # 3. Group tranches by deal, compute per-deal / per-tranche inputs
    # ------------------------------------------------------------------
    print("[6/7] Computing per-deal prophet claims, multipliers, adjust factors...")

    deal_to_tranches = {}
    for tranche_name in comprehensive_results:
        deal = get_deal_name_from_rga_tab(tranche_name)
        deal_to_tranches.setdefault(deal, []).append(tranche_name)

    val_month_end = _get_month_end_timestamp(VALUATION_DATE)

    # Prophet claims / rga_share per deal
    adjusted_prophet_claims = {}
    for deal_name in deal_to_tranches:
        rga_share = RGA_SHARE_BY_DEAL.get(deal_name, 1.0)
        adjusted_prophet_claims[deal_name] = {}
        be_raw = get_prophet_claims_for_deal(prophet_be_df, deal_name)
        adjusted_prophet_claims[deal_name]["BE"] = be_raw / rga_share
        stress_raw = get_prophet_claims_for_deal(prophet_stress_df, deal_name)
        adjusted_prophet_claims[deal_name]["1in200"] = stress_raw / rga_share

    # Tranche multipliers
    deal_multipliers = {}
    for deal_name in deal_to_tranches:
        deal_multipliers[deal_name] = calculate_tranche_multipliers(
            comprehensive_results, VALUATION_DATE, deal_name
        )

    # adjust_to_actual_claim per tranche
    tranche_adjust_factors = {}
    for deal_name, tranche_list in deal_to_tranches.items():
        be_claims = adjusted_prophet_claims[deal_name]["BE"]
        for tranche_name in tranche_list:
            tranche_df = comprehensive_results.get(tranche_name)
            mult = deal_multipliers.get(deal_name, {}).get(tranche_name, 1.0)
            tranche_adjust_factors[tranche_name] = _compute_adjust_to_actual_claim(
                tranche_df, be_claims, mult, VALUATION_DATE
            )

    # Pre-compute tranche arrays
    tranche_arrays_cache = {}
    tranche_inception_dates = {}
    for tranche_name, df in comprehensive_results.items():
        if df is None or df.empty:
            continue
        arrays = _prepare_tranche_arrays(df, VALUATION_DATE)
        if arrays is not None:
            tranche_arrays_cache[tranche_name] = arrays
            valid = [pd.Timestamp(d) for d in arrays["date_datetimes"] if pd.notna(d)]
            if valid:
                tranche_inception_dates[tranche_name] = min(valid)

    # ------------------------------------------------------------------
    # 4. Run chosen scenario/offset for each tranche
    # ------------------------------------------------------------------
    print(f"[7/7] Computing factors + AFV for each tranche "
          f"(Scenario {SCENARIO_NUMBER}, offset={TARGET_OFFSET})...")
    print()

    offset_date_str = get_offset_date(VALUATION_DATE, TARGET_OFFSET)
    anchor_date_ts = (val_month_end.to_period("M") + TARGET_OFFSET).to_timestamp("M")

    all_rows = []  # for CSV export

    for deal_name in sorted(deal_to_tranches.keys()):
        tranche_list = deal_to_tranches[deal_name]
        rga_share = RGA_SHARE_BY_DEAL.get(deal_name, 1.0)

        for tranche_name in sorted(tranche_list):
            if tranche_name not in tranche_arrays_cache:
                print(f"  SKIP {tranche_name} -- no cached arrays")
                continue

            arrays = tranche_arrays_cache[tranche_name]
            inception_date = tranche_inception_dates.get(tranche_name)
            if inception_date is None:
                print(f"  SKIP {tranche_name} -- no inception date")
                continue

            multiplier = deal_multipliers.get(deal_name, {}).get(tranche_name, 1.0)
            be_claims = adjusted_prophet_claims[deal_name]["BE"]
            stress_claims = adjusted_prophet_claims[deal_name]["1in200"]

            # Tranche-specific inflation
            tranche_infl_lookup, tranche_valdate_infl = _get_tranche_inflation_lookup(
                comprehensive_results, tranche_name, VALUATION_DATE
            )

            adjust_factor = tranche_adjust_factors.get(tranche_name, 1.0)

            # Pre-compute blending arrays
            base_arr, delta_arr = _precompute_blending_arrays(
                arrays,
                be_claims,
                stress_claims,
                multiplier,
                tranche_infl_lookup,
                tranche_valdate_infl,
                adjust_to_actual_claim=adjust_factor,
            )

            # Blended claims
            blended_claims = base_arr + blend_factor * delta_arr

            # Find anchor index
            anchor_idx = arrays["date_to_idx"].get(anchor_date_ts, -1)
            if anchor_idx < 0:
                for i, dt in enumerate(arrays["date_datetimes"]):
                    if pd.notna(dt) and pd.Timestamp(dt) >= anchor_date_ts:
                        anchor_idx = i
                        break

            if anchor_idx < 0 or anchor_idx >= arrays["n_rows"]:
                print(f"  SKIP {tranche_name} -- anchor index out of range")
                continue

            # Compute factors WITH X values
            factors = compute_factors_with_x_values(
                arrays, anchor_date_ts, inception_date, blended_claims
            )

            # Compute adjusted_float_vector
            afv = _compute_adjusted_float_vector_fast(arrays, factors, anchor_idx)

            # Dates for AFV entries
            afv_dates = arrays["dates"][anchor_idx : anchor_idx + len(afv)]

            # ---- Print results ----
            print("-" * 70)
            print(f"TRANCHE: {tranche_name}  (Deal: {deal_name})")
            print(f"  RGA Share           : {rga_share}")
            print(f"  Multiplier          : {multiplier:.10f}")
            print(f"  Adjust-to-actual    : {adjust_factor:.10f}")
            print(f"  Inception date      : {inception_date}")
            print(f"  Anchor date         : {anchor_date_ts}")
            print(f"  Anchor index        : {anchor_idx}")
            print(f"  Valdate inflation   : {tranche_valdate_infl:.10f}")
            print()
            print(f"  X VALUES (from c66 and c6 lookups):")
            print(f"    c66_actual  = {factors['c66_actual']}")
            print(f"    c6_actual   = {factors['c6_actual']}")
            print(f"    c66_fixed   = {factors['c66_fixed']}")
            print(f"    c6_fixed    = {factors['c6_fixed']}")
            print(f"    c66_inc     = {factors['c66_increased']}")
            print(f"    c6_inc      = {factors['c6_increased']}")
            print(f"    c66_dec     = {factors['c66_decreased']}")
            print(f"    c6_dec      = {factors['c6_decreased']}")
            print()

            def _fmt(v):
                return f"{v:.12f}" if v is not None else "None"

            print(f"    X_Actual             : {_fmt(factors['X_Actual'])}")
            print(f"    X_Fixed              : {_fmt(factors['X_Fixed'])}")
            print(f"    X_Increased          : {_fmt(factors['X_Increased'])}")
            print(f"    X_Decreased          : {_fmt(factors['X_Decreased'])}")
            print()
            print(f"  DERIVED FACTORS:")
            print(f"    Experience_Raw       : {factors['experience_raw']:.12f}")
            print(f"    Credibility_Factor   : {factors['Credibility_Factor']:.12f}")
            print(f"    Experience_Factor    : {factors['Experience_Factor']:.12f}")
            print(f"    Interpolation_Vector : {factors['Interpolation_Vector']}")
            print(f"    Adjustment_Factor    : {factors['Adjustment_Factor']:.12f}")
            print(f"      (sum_claims={factors['sum_claims_adj']:.6f}, "
                  f"sum_fixed={factors['sum_fixed_adj']:.6f}, "
                  f"sum_inc={factors['sum_increased_adj']:.6f}, "
                  f"sum_dec={factors['sum_decreased_adj']:.6f})")
            print()
            print(f"  ADJUSTED_FLOAT_VECTOR  (length={len(afv)}, sum={np.nansum(afv):.6f}):")

            n_print = min(PRINT_HEAD, len(afv))
            for j in range(n_print):
                date_label = afv_dates[j] if j < len(afv_dates) else "?"
                print(f"    [{j:4d}] {date_label}  ->  {afv[j]:.10f}")
            if len(afv) > PRINT_HEAD:
                print(f"    ... ({len(afv) - PRINT_HEAD} more values)")
            print()

            # Collect rows for CSV
            for j in range(len(afv)):
                date_label = afv_dates[j] if j < len(afv_dates) else ""
                all_rows.append({
                    "Deal": deal_name,
                    "Tranche": tranche_name,
                    "Scenario": scenario_name,
                    "Offset": TARGET_OFFSET,
                    "Blend_Factor": blend_factor,
                    "Anchor_Date": str(anchor_date_ts.date()),
                    "AFV_Index": j,
                    "Date": date_label,
                    "Adjusted_Float_Vector": afv[j],
                    "X_Actual": factors["X_Actual"],
                    "X_Fixed": factors["X_Fixed"],
                    "X_Increased": factors["X_Increased"],
                    "X_Decreased": factors["X_Decreased"],
                    "Experience_Factor": factors["Experience_Factor"],
                    "Interpolation_Vector": factors["Interpolation_Vector"],
                    "Adjustment_Factor": factors["Adjustment_Factor"],
                    "Credibility_Factor": factors["Credibility_Factor"],
                    "Multiplier": multiplier,
                    "Adjust_to_Actual": adjust_factor,
                    "RGA_Share": rga_share,
                })

    # ------------------------------------------------------------------
    # 5. Export to CSV
    # ------------------------------------------------------------------
    if OUTPUT_CSV and all_rows:
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        df_out = pd.DataFrame(all_rows)
        df_out.to_csv(OUTPUT_CSV, index=False)
        print("=" * 70)
        print(f"Exported {len(df_out)} rows to {OUTPUT_CSV}")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
