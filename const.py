"""
CONSTANTS FILE FOR UK LONGEVITY SWAP CALCULATOR
================================================
Contains all configuration, mappings, and constants.
"""








# Tranches with deferred/pensioner calculations
DEFERRED_PENSIONER_TRANCHES = [
   "Titan_ROL",
   "Paternoster_PDU",
   "Paternoster_EAP"
]




# Mapping between deal names in this tool and Prophet column headers
PROPHET_DEAL_NAME_MAPPING = {
   "Artemis": "Rothesay",
   "Excalibur": "Rothesay_Excalibur",
   "Lancelot": "Rothesay_Lancelot",
   "Paternoster": "Paternoster",
   "Romeo": "Romeo",
   "Titan": "Titan",
   "Advance_T1a": "Paternoster",
   "Advance_T1b": "Paternoster",
   "Antigua": "Paternoster",
   "Jupiter": "Rothesay_Jupiter",
   "Laker_T1": "Paternoster",
   "Sherwood_Forest": "Paternoster",
}




# Prophet sheet names by scenario type
PROPHET_SHEETS = {
   "BE": "Longevity_GFS_RM",
   "1in200": "Longevity_OVR_TOR9",
   "1in20": "Longevity_OVR_TOR3",
}




# Sensitivity scenario configuration
SENSITIVITY_SCENARIOS = [
   {"name": "Scenario 1", "mortality": "BE", "grading_period": 0, "discount_shock": 0.0},
   {"name": "Scenario 2", "mortality": "1in200", "grading_period": 60, "discount_shock": 0.0},
   {"name": "Scenario 3", "mortality": "1in200", "grading_period": 60, "discount_shock": 0.013411942},
   {"name": "Scenario 4", "mortality": "1in200", "grading_period": 60, "discount_shock": 0.011564121},
   {"name": "Scenario 5", "mortality": "1in200", "grading_period": 60, "discount_shock": -0.003345999},
   {"name": "Scenario 6", "mortality": "1in200", "grading_period": 60, "discount_shock": -0.009482681},
]




# Max months for sensitivity offsets
SENSITIVITY_MAX_MONTHS = 500








# RGA to Client tab mapping
RGA_TO_CLIENT_MAPPING = {
   # Artemis
   "Artemis_ART": "ART_RGA_GLB + ART_RGA_INT",








   # Excalibur
   "Excalibur_C03_AME": "Excalibur_C03_RGAAm",
   "Excalibur_C03_INT": "Excalibur_C03_RGAIn",
   "Excalibur_C05_AME": "Excalibur_C05_RGAAm",
   "Excalibur_C05_INT": "Excalibur_C05_RGAIn",
   "Excalibur_L05_AME": "Excalibur_L05_RGAAm",
   "Excalibur_FIX_AME": "Excalibur_Fix_RGAAm",
   "Excalibur_FIX_INT": "Excalibur_Fix_RGAIn",
   "Excalibur_L02_AME": "Excalibur_L02_RGAAm",
   "Excalibur_L02_INT": "Excalibur_L02_RGAIn",
   "Excalibur_L05_INT": "Excalibur_L05_RGAIn",
   "Excalibur_L06_AME": "Excalibur_L06_RGAAm",
   "Excalibur_L06_INT": "Excalibur_L06_RGAIn",
   "Excalibur_L35_AME": "Excalibur_L35_RGAAm",
   "Excalibur_L35_INT": "Excalibur_L35_RGAIn",








   # Lancelot
   "Lancelot_001_INT": "Lance_001_RGAIn",
   "Lancelot_001_AME": "Lance_001_RGAAm",
   "Lancelot_004_AME": "Lance_004_RGAAm",
   "Lancelot_004_INT": "Lance_004_RGAIn",
   "Lancelot_005_AME": "Lance_005_RGAAm",
   "Lancelot_005_INT": "Lance_005_RGAIn",
   "Lancelot_006_AME": "Lance_006_RGAAm",
   "Lancelot_006_INT": "Lance_006_RGAIn",
   "Lancelot_FIX_AME": "Lance_Fix_RGAAm",
   "Lancelot_FIX_INT": "Lance_Fix_RGAIn",








   # Paternoster
   "Paternoster_MOR": "Titan_MOR_RGAGl + Titan_MOR_RGAIn",
   "Paternoster_PDU": "Titan_PDU_RGAGl + Titan_PDU_RGAIn",
   "Paternoster_EAP": "Titan_EAP_RGAGl + Titan_EAP_RGAIn",
   "Paternoster_PAO": "Titan_PAO_RGAGl + Titan_PAO_RGAIn",
   "Paternoster_MET": "Titan_MET_RGAGl + Titan_MET_RGAIn",
   "Paternoster_TIG": "Titan_TIG_RGAGl + Titan_TIG_RGAIn",
   "Paternoster_PTR": "Titan_PTR_RGAGl + Titan_PTR_RGAIn",
   "Paternoster_LMN": "Titan_LMN_RGAGl + Titan_LMN_RGAIn",
   "Paternoster_HDA": "Titan_HDA_RGAGl + Titan_HDA_RGAIn",








   # Romeo
   "Romeo_RAD": "Romeo_RAD_Pen_RGG + Romeo_RAD_Pen_RGI",
   "Romeo_VST": "Romeo_VST_Pen_RGG + Romeo_VST_Pen_RGI",
   "Romeo_TI3": "Romeo_TI3_Pen_RGG + Romeo_TI3_Pen_RGI",
   "Romeo_COB": "Romeo_COB_Pen_RGG + Romeo_COB_Pen_RGI",
   "Romeo_IHG": "Romeo_IHG_Pen_RGG + Romeo_IHG_Pen_RGI",
   "Romeo_GMR": "Romeo_GMR_Pen_RGG + Romeo_GMR_Pen_RGI",
   "Romeo_BER": "Romeo_BER_Pen_RGG + Romeo_BER_Pen_RGI",
   "Romeo_GKN": "Romeo_GKN_Pen_RGG + Romeo_GKN_Pen_RGI",








   # Titan
   "Titan_CDC": "Titan_CDC_RGAGl + Titan_CDC_RGAIn",
   "Titan_ROL": "Titan_ROL_RGAGl + Titan_ROL_RGAIn",








   # Advance T1a
   "Advance_T1a_L02_Am": "AdvanceT1A_L02_RGAAm",
   "Advance_T1a_L02_In": "AdvanceT1A_L02_RGAIn",
   "Advance_T1a_L05_Am": "AdvanceT1A_L05_RGAAm",
   "Advance_T1a_L05_In": "AdvanceT1A_L05_RGAIn",








   # Advance T1b
   "Advance_T1b_C03_Am": "AdvanceT1B_C03_RGAAm",
   "Advance_T1b_C03_In": "AdvanceT1B_C03_RGAIn",
   "Advance_T1b_C05_Am": "AdvanceT1B_C05_RGAAm",
   "Advance_T1b_C05_In": "AdvanceT1B_C05_RGAIn",
   "Advance_T1b_Fx0_Am": "AdvanceT1B_Fx0_RGAAm",
   "Advance_T1b_Fx0_In": "AdvanceT1B_Fx0_RGAIn",
   "Advance_T1b_Fx5_Am": "AdvanceT1B_Fx5_RGAAm",
   "Advance_T1b_Fx5_In": "AdvanceT1B_Fx5_RGAIn",
   "Advance_T1b_L03_Am": "AdvanceT1B_L03_RGAAm",
   "Advance_T1b_L03_In": "AdvanceT1B_L03_RGAIn",








   # Antigua
   "Antigua_Antigua_L10_RGAIn": "Antigua_L10_RGAIn",
   "Antigua_Antigua_L10_RGAAm": "Antigua_L10_RGAAm",
   "Antigua_Antigua_C03_RGAIn": "Antigua_C03_RGAIn",
   "Antigua_Antigua_C03_RGAAm": "Antigua_C03_RGAAm",
   "Antigua_Antigua_Fix_RGAIn": "Antigua_Fix_RGAIn",
   "Antigua_Antigua_Fix_RGAAm": "Antigua_Fix_RGAAm",








   # Jupiter
   "Jupiter_C03_RGAAm": "Jupiter_C03_RGAAm",
   "Jupiter_C03_RGAIn": "Jupiter_C03_RGAIn",
   "Jupiter_L05_RGAIn": "Jupiter_L05_RGAIn",
   "Jupiter_L05_RGAAm": "Jupiter_L05_RGAAm",
   "Jupiter_L07_RGAIn": "Jupiter_L07_RGAIn",
   "Jupiter_L07_RGAAm": "Jupiter_L07_RGAAm",
   "Jupiter_L10_RGAIn": "Jupiter_L10_RGAIn",
   "Jupiter_L10_RGAAm": "Jupiter_L10_RGAAm",
   "Jupiter_Fix_RGAAm": "Jupiter_Fix_RGAAm",
   "Jupiter_Fix_RGAIn": "Jupiter_Fix_RGAIn",








   # Laker T1
   "Laker_T1_Fx0_Am": "LakerT1_Fx0_RGAAm",
   "Laker_T1_Fx0_In": "LakerT1_Fx0_RGAIn",
   "Laker_T1_Fx3_Am": "LakerT1_Fx3_RGAAm",
   "Laker_T1_Fx3_In": "LakerT1_Fx3_RGAIn",








   # Sherwood Forest
   "Sherwood_Forest_Sher_001_AME": "Sher_001_RGAAm",
   "Sherwood_Forest_Sher_001_INT": "Sher_001_RGAIn",
   "Sherwood_Forest_Sher_002_AME": "Sher_002_RGAAm",
   "Sherwood_Forest_Sher_002_INT": "Sher_002_RGAIn",
   "Sherwood_Forest_Sher_003_AME": "Sher_003_RGAAm",
   "Sherwood_Forest_Sher_003_INT": "Sher_003_RGAIn",
   "Sherwood_Forest_Sher_004_AME": "Sher_004_RGAAm",
   "Sherwood_Forest_Sher_004_INT": "Sher_004_RGAIn",
   "Sherwood_Forest_Forest_001_AME": "Forest_001_RGAAm",
   "Sherwood_Forest_Forest_001_INT": "Forest_001_RGAIn",
   "Sherwood_Forest_Forest_002_AME": "Forest_002_RGAAm",
   "Sherwood_Forest_Forest_002_INT": "Forest_002_RGAIn",
   "Sherwood_Forest_Forest_003_AME": "Forest_003_RGAAm",
   "Sherwood_Forest_Forest_003_INT": "Forest_003_RGAIn",
   "Sherwood_Forest_Forest_004_AME": "Forest_004_RGAAm",
   "Sherwood_Forest_Forest_004_INT": "Forest_004_RGAIn",
   "Sherwood_Forest_FrstNMA_001_AME": "FrstNMA_001_RGAAm",
   "Sherwood_Forest_FrstNMA_001_INT": "FrstNMA_001_RGAIn",
   "Sherwood_Forest_FrstNMA_002_AME": "FrstNMA_002_RGAAm",
   "Sherwood_Forest_FrstNMA_002_INT": "FrstNMA_002_RGAIn",
   "Sherwood_Forest_FrstNMA_003_AME": "FrstNMA_003_RGAAm",
   "Sherwood_Forest_FrstNMA_003_INT": "FrstNMA_003_RGAIn",
   "Sherwood_Forest_FrstNMA_004_AME": "FrstNMA_004_RGAAm",
   "Sherwood_Forest_FrstNMA_004_INT": "FrstNMA_004_RGAIn"
}








# RGA share percentages by deal
RGA_SHARE_BY_DEAL = {
   "Artemis": 0.0140,
   "Excalibur": 1.0000,
   "Lancelot": 1.0000,
   "Paternoster": 0.46015,
   "Romeo": 0.2000,
   "Titan": 0.46015,
   "Advance_T1a": 1.0000,
   "Advance_T1b": 1.0000,
   "Antigua": 1.0000,
   "Jupiter": 1.0000,
   "Laker_T1": 1.0000,
   "Sherwood_Forest": 1.0000
}








# Standard Gross-Up Factor by deal (for regular tranches)
GROSS_UP_FACTOR_BY_DEAL = {
   "Artemis": 0.0575,
   "Excalibur": 0.0425,
   "Lancelot": 0.0275,
   "Paternoster": 0.0495,
   "Romeo": 0.0500,
   "Titan": 0.0495,
   "Advance_T1a": 0.0373,
   "Advance_T1b": 0.0185,
   "Antigua": 0.0200,
   "Jupiter": 0.0455,
   "Laker_T1": 0.0195,
   "Sherwood_Forest": 0.0510
}








# Gross-Up Factor for Pensioner (for deferred/pensioner tranches)
GROSS_UP_FACTOR_PENSIONER = {
   "Artemis": 0.0575,
   "Excalibur": 0.0425,
   "Lancelot": 0.0275,
   "Paternoster": 0.0495,
   "Romeo": 0.0500,
   "Titan": 0.0495,
   "Advance_T1a": 0.0373,
   "Advance_T1b": 0.0185,
   "Antigua": 0.0200,
   "Jupiter": 0.0455,
   "Laker_T1": 0.0195,
   "Sherwood_Forest": 0.0510
}








# Gross-Up Factor for Deferred (for deferred/pensioner tranches)
GROSS_UP_FACTOR_DEFERRED = {
   "Artemis": 0.0575,
   "Excalibur": 0.0425,
   "Lancelot": 0.0275,
   "Paternoster": 0.0945,
   "Romeo": 0.0500,
   "Titan": 0.0945,
   "Advance_T1a": 0.0373,
   "Advance_T1b": 0.0185,
   "Antigua": 0.0200,
   "Jupiter": 0.0455,
   "Laker_T1": 0.0195,
   "Sherwood_Forest": 0.0510
}








# Discount curve configuration
DISCOUNT_CURVE_CONFIG = {
   "MAX_MONTHS": 880,
   "SPREAD": 0.0,
   "SENSITIVITY_SHOCK": 0.0,
   "BASE_ADJUSTMENT": 0.0025,
}








# SONIA configuration
SONIA_SHEET_NAME = "SONIA"








# Fee tab mapping for deals with additional fee vectors
FEE_TAB_MAPPING = {
   "Advance_T1a": "AdvanceT1A_Fee_RGAIn + AdvanceT1A_Fee_RGAAm",
   "Advance_T1b": "AdvanceT1B_Fee_RGAIn + AdvanceT1B_Fee_RGAAm"
}








# Output column order
OUTPUT_COLUMNS_ORDER = [
   'Date',
   'Fixed_Vectors', 'Increase_Vectors', 'Decrease_Vectors',
   'Fixed_deferred', 'Fixed_pensioner',
   'Historical_Infl_Factors', 'Projected_Infl_Factors',
   'Total_Actual_Claims',
   'Fixed_w_Original', 'Increased_w_Original', 'Decreased_w_Original',
   'Fixed_w_Real', 'Increased_w_Real', 'Decreased_w_Real',
   'Deferred_w_Real', 'Pensioner_w_Real',
   'Experience_Factor', 'Interpolation_Vector', 'Adjustment_Factor', 'Credibility_Factor',
   'Float_Vector', 'Adjusted_Float_Vector',
]
















def get_deal_name_from_rga_tab(rga_tab_name):
   """Extract deal name from RGA tab name."""
   if rga_tab_name.startswith("Antigua_Antigua"):
       return "Antigua"
   elif rga_tab_name.startswith("Sherwood_Forest"):
       return "Sherwood_Forest"
   elif rga_tab_name.startswith("Advance_T1a"):
       return "Advance_T1a"
   elif rga_tab_name.startswith("Advance_T1b"):
       return "Advance_T1b"
   elif rga_tab_name.startswith("Laker_T1"):
       return "Laker_T1"
   else:
       return rga_tab_name.split('_')[0]
















def get_rga_share(rga_tab_name):
   """Get RGA share percentage for a given RGA tab name."""
   deal_name = get_deal_name_from_rga_tab(rga_tab_name)
   return RGA_SHARE_BY_DEAL.get(deal_name, None)
















def get_gross_up_factor(deal_name):
   """Get standard Gross-Up Factor for a given deal name."""
   return GROSS_UP_FACTOR_BY_DEAL.get(deal_name, None)
















def get_gross_up_factor_pensioner(deal_name):
   """Get Pensioner Gross-Up Factor for a given deal name."""
   return GROSS_UP_FACTOR_PENSIONER.get(deal_name, None)
















def get_gross_up_factor_deferred(deal_name):
   """Get Deferred Gross-Up Factor for a given deal name."""
   return GROSS_UP_FACTOR_DEFERRED.get(deal_name, None)
















def is_deferred_pensioner_tranche(rga_tab_name):
   """Check if tranche uses deferred/pensioner calculations."""
   return rga_tab_name in DEFERRED_PENSIONER_TRANCHES













