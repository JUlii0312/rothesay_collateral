"""
ROTHESAY COLLATERAL CALCULATION
============================
Main entry point with GUI interface.




Run this script to launch the calculator with a popup window
for entering configuration parameters.
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
import os
import time

from logging_config import (
    setup_logging,
    get_logger,
    log_section_start,
    log_section_end,
    log_subsection,
    log_file_operation,
    log_calculation_result,
    log_execution_time,
    ProgressTracker,
)
from const import DISCOUNT_CURVE_CONFIG
from client_data_extractor import (
    load_fixed_vectors_data,
    extract_client_data,
    extract_client_discount_factors,
    extract_client_exposure_summary,
    extract_fee_vectors,
    extract_fee_pv_values,
)
from calculation import (
    load_sonia_rates,
    build_rga_discount_curve,
    calculate_all_tranches,
    run_sensitivity_runs,
)
from output_formatter import (
    write_inflation_adjusted_output,
    create_valdate_output,
    write_sensitivity_output,
)

# Initialize logger for this module
logger = get_logger("main")


class CalculatorGUI:
    """GUI for Rothesay Collateral Calculation."""

    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Rothesay Collateral Calculation")
        self.root.geometry("750x520")
        self.root.resizable(False, False)

        self._create_widgets()
        self._center_window()

    def _center_window(self):
        """Center the window on screen."""
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f'{width}x{height}+{x}+{y}')

    def _create_widgets(self):
        """Create GUI widgets."""
        main_frame = ttk.Frame(self.root, padding="20")
        main_frame.grid(row=0, column=0, sticky="nsew")

        # Title
        title_label = ttk.Label(main_frame, text="Rothesay Collateral Calculation",
                                font=('Helvetica', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))

        # Valuation Date
        ttk.Label(main_frame, text="Valuation Date (YYYY-MM-DD):").grid(
            row=1, column=0, sticky="w", pady=5)
        self.val_date_var = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        self.val_date_entry = ttk.Entry(main_frame, textvariable=self.val_date_var, width=50)
        self.val_date_entry.grid(row=1, column=1, columnspan=2, sticky="w", pady=5)

        # Client File Path
        ttk.Label(main_frame, text="Client File Path:").grid(
            row=2, column=0, sticky="w", pady=5)
        self.client_file_var = tk.StringVar(value="G:/Not treaty specific/TRTYREVW/UK Longevity Swap/Collateral Calculation/2025/9_September 30 2025/Rothesay Calls/Collateral Supporting Information RGA 01Oct2025_KS.xlsx")
        self.client_file_entry = ttk.Entry(main_frame, textvariable=self.client_file_var, width=50)
        self.client_file_entry.grid(row=2, column=1, sticky="w", pady=5)
        ttk.Button(main_frame, text="Browse...", command=self._browse_client_file).grid(
            row=2, column=2, padx=5, pady=5)

        # Fixed Vector File Path
        ttk.Label(main_frame, text="Fixed Vector File Path:").grid(
            row=3, column=0, sticky="w", pady=5)
        self.fixed_vector_var = tk.StringVar(value="G:/Not treaty specific/TRTYREVW/UK Longevity Swap/Collateral Calculation/2025/11_November 30 2025/Rothesay Calls/Nov_Reconcile/Fixed_Vector_Consolidated.xlsx")
        self.fixed_vector_entry = ttk.Entry(main_frame, textvariable=self.fixed_vector_var, width=50)
        self.fixed_vector_entry.grid(row=3, column=1, sticky="w", pady=5)
        ttk.Button(main_frame, text="Browse...", command=self._browse_fixed_vector_file).grid(
            row=3, column=2, padx=5, pady=5)

        # SONIA File Path
        ttk.Label(main_frame, text="SONIA Rates File Path:").grid(
            row=4, column=0, sticky="w", pady=5)
        self.sonia_file_var = tk.StringVar(value="G:/Not treaty specific/TRTYREVW/UK Longevity Swap/Collateral Calculation/Monthly Swap Rate/Monthly Swap Rates Summary_2026.xlsx")
        self.sonia_file_entry = ttk.Entry(main_frame, textvariable=self.sonia_file_var, width=50)
        self.sonia_file_entry.grid(row=4, column=1, sticky="w", pady=5)
        ttk.Button(main_frame, text="Browse...", command=self._browse_sonia_file).grid(
            row=4, column=2, padx=5, pady=5)

        # Output Option
        ttk.Label(main_frame, text="Output Option:").grid(
            row=5, column=0, sticky="w", pady=10)
        self.output_option_var = tk.StringVar(value="valdate_only")
        output_frame = ttk.Frame(main_frame)
        output_frame.grid(row=5, column=1, columnspan=2, sticky="w", pady=10)

        ttk.Radiobutton(output_frame, text="Val_Date Output Only",
                        variable=self.output_option_var, value="valdate_only").pack(anchor="w")
        ttk.Radiobutton(output_frame, text="Both Val_Date Output and Inflation_Adjusted Output",
                        variable=self.output_option_var, value="both").pack(anchor="w")

        # Run Mode
        ttk.Label(main_frame, text="Run Mode:").grid(
            row=6, column=0, sticky="w", pady=10)
        self.run_mode_var = tk.StringVar(value="standard")
        run_mode_frame = ttk.Frame(main_frame)
        run_mode_frame.grid(row=6, column=1, columnspan=2, sticky="w", pady=10)
        ttk.Radiobutton(run_mode_frame, text="Standard",
                        variable=self.run_mode_var, value="standard").pack(anchor="w")
        ttk.Radiobutton(run_mode_frame, text="Run Sensitivity",
                        variable=self.run_mode_var, value="run_sensitivity").pack(anchor="w")

        # Prophet Output File Path
        ttk.Label(main_frame, text="Prophet Output File Path:").grid(
            row=7, column=0, sticky="w", pady=5)
        self.prophet_file_var = tk.StringVar(value="G:/Not treaty specific/TRTYREVW/UK Longevity Swap/Demographics/2025_Q3/2025Q3/Prophet Output/20250930 Prophet Query - Longevity Deflated.xlsx")
        self.prophet_file_entry = ttk.Entry(main_frame, textvariable=self.prophet_file_var, width=50)
        self.prophet_file_entry.grid(row=7, column=1, sticky="w", pady=5)
        ttk.Button(main_frame, text="Browse...", command=self._browse_prophet_file).grid(
            row=7, column=2, padx=5, pady=5)

        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.grid(row=8, column=0, columnspan=3, pady=30)

        ttk.Button(button_frame, text="Run Calculator", command=self._run_calculator,
                   width=20).pack(side="left", padx=10)
        ttk.Button(button_frame, text="Cancel", command=self.root.destroy,
                   width=20).pack(side="left", padx=10)

        # Status
        self.status_var = tk.StringVar(value="Ready")
        status_label = ttk.Label(main_frame, textvariable=self.status_var,
                                 font=('Helvetica', 10, 'italic'))
        status_label.grid(row=9, column=0, columnspan=3, pady=10)

    def _browse_client_file(self):
        """Browse for client file."""
        filename = filedialog.askopenfilename(
            title="Select Client File",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")]
        )
        if filename:
            self.client_file_var.set(filename)

    def _browse_fixed_vector_file(self):
        """Browse for fixed vector file."""
        filename = filedialog.askopenfilename(
            title="Select Fixed Vector File",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
        )
        if filename:
            self.fixed_vector_var.set(filename)

    def _browse_sonia_file(self):
        """Browse for SONIA rates file."""
        filename = filedialog.askopenfilename(
            title="Select SONIA Rates File",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")]
        )
        if filename:
            self.sonia_file_var.set(filename)

    def _browse_prophet_file(self):
        """Browse for Prophet output file."""
        filename = filedialog.askopenfilename(
            title="Select Prophet Output File",
            filetypes=[("Excel files", "*.xlsx *.xlsm"), ("All files", "*.*")]
        )
        if filename:
            self.prophet_file_var.set(filename)

    def _validate_inputs(self):
        """Validate user inputs."""
        errors = []

        # Validate date
        try:
            datetime.strptime(self.val_date_var.get(), "%Y-%m-%d")
        except ValueError:
            errors.append("Invalid date format. Use YYYY-MM-DD.")

        # Validate file paths
        if not self.client_file_var.get():
            errors.append("Client file path is required.")
        elif not os.path.exists(self.client_file_var.get()):
            errors.append("Client file not found.")

        if not self.fixed_vector_var.get():
            errors.append("Fixed vector file path is required.")
        elif not os.path.exists(self.fixed_vector_var.get()):
            errors.append("Fixed vector file not found.")

        if self.sonia_file_var.get() and not os.path.exists(self.sonia_file_var.get()):
            errors.append("SONIA file not found.")

        # Prophet file required for Run Sensitivity
        if self.run_mode_var.get() == "run_sensitivity":
            if not self.prophet_file_var.get():
                errors.append("Prophet output file path is required for Run Sensitivity.")
            elif not os.path.exists(self.prophet_file_var.get()):
                errors.append("Prophet output file not found.")
            if not self.sonia_file_var.get():
                errors.append("SONIA file path is required for Run Sensitivity.")

        return errors

    def _run_calculator(self):
        """Run the calculator with provided inputs."""
        errors = self._validate_inputs()
        if errors:
            messagebox.showerror("Validation Error", "\n".join(errors))
            return

        self.status_var.set("Processing...")
        self.root.update()

        try:
            run_calculation(
                valuation_date=self.val_date_var.get(),
                client_file_path=self.client_file_var.get(),
                fixed_vector_file_path=self.fixed_vector_var.get(),
                sonia_file_path=self.sonia_file_var.get() or None,
                output_both=self.output_option_var.get() == "both",
                run_sensitivity=self.run_mode_var.get() == "run_sensitivity",
                prophet_file_path=self.prophet_file_var.get() or None
            )
            self.status_var.set("Completed successfully!")
            messagebox.showinfo("Success", "Calculation completed successfully!")

        except Exception as e:
            self.status_var.set("Error occurred")
            logger.exception("Calculation failed with exception")
            messagebox.showerror("Error", f"An error occurred:\n{str(e)}")

    def run(self):
        """Run the GUI."""
        self.root.mainloop()


@log_execution_time
def run_calculation(valuation_date, client_file_path, fixed_vector_file_path,
                    sonia_file_path=None, output_both=False,
                    run_sensitivity=False, prophet_file_path=None):
    """
    Run the complete calculation workflow.

    Args:
        valuation_date: Valuation date string (YYYY-MM-DD)
        client_file_path: Path to client Excel file
        fixed_vector_file_path: Path to Fixed_Vector_Consolidated.xlsx
        sonia_file_path: Path to SONIA rates file (optional)
        output_both: If True, output both files; if False, only Val_Date Output
        run_sensitivity: If True, also run sensitivity scenarios
        prophet_file_path: Path to Prophet output Excel file (required for sensitivity)
    """
    # Initialize logging with file output
    setup_logging(log_dir="data/output/logs")
    
    calculation_start_time = time.time()
    
    log_section_start(logger, "Rothesay Collateral Calculation")
    logger.info(f"Valuation Date: {valuation_date}")
    logger.info(f"Output Mode: {'Both outputs' if output_both else 'Val_Date only'}")
    logger.info(f"Sensitivity Mode: {'Enabled' if run_sensitivity else 'Disabled'}")
    
    # Log input file paths
    log_subsection(logger, "Input Files Configuration")
    log_file_operation(logger, "INPUT", client_file_path, os.path.exists(client_file_path))
    log_file_operation(logger, "INPUT", fixed_vector_file_path, os.path.exists(fixed_vector_file_path))
    if sonia_file_path:
        log_file_operation(logger, "INPUT", sonia_file_path, os.path.exists(sonia_file_path))
    if prophet_file_path:
        log_file_operation(logger, "INPUT", prophet_file_path, os.path.exists(prophet_file_path))

    # =========================================================================
    # STEP 1: Load Fixed Vectors Data
    # =========================================================================
    log_subsection(logger, "Step 1: Loading Fixed Vectors Data")
    step_start = time.time()
    
    fixed_vectors_data = load_fixed_vectors_data(fixed_vector_file_path)
    if not fixed_vectors_data:
        logger.error("Could not load Fixed Vectors data - aborting")
        raise ValueError("Could not load Fixed Vectors data")
    
    logger.info(f"Step 1 completed in {time.time() - step_start:.2f}s")
    log_calculation_result(logger, "Fixed Vector Datasets Loaded", len(fixed_vectors_data))

    # =========================================================================
    # STEP 2: Extract Client Data
    # =========================================================================
    log_subsection(logger, "Step 2: Extracting Client Data")
    step_start = time.time()
    
    client_claims_data, client_vectors_data = extract_client_data(client_file_path)
    if not client_vectors_data:
        logger.error("Could not extract Client vectors data - aborting")
        raise ValueError("Could not extract Client vectors data")
    
    logger.info(f"Step 2 completed in {time.time() - step_start:.2f}s")
    log_calculation_result(logger, "Client Claims Datasets", len(client_claims_data))
    log_calculation_result(logger, "Client Vectors Datasets", len(client_vectors_data))

    # =========================================================================
    # STEP 3: Extract Fee Data
    # =========================================================================
    log_subsection(logger, "Step 3: Extracting Fee Vectors and PV Values")
    step_start = time.time()
    
    fee_vectors_data = extract_fee_vectors(client_file_path)
    fee_pv_data = extract_fee_pv_values(client_file_path)
    
    logger.info(f"Step 3 completed in {time.time() - step_start:.2f}s")
    log_calculation_result(logger, "Fee Vector Datasets", len(fee_vectors_data))
    log_calculation_result(logger, "Fee PV Datasets", len(fee_pv_data))

    # =========================================================================
    # STEP 4: Calculate All Tranches
    # =========================================================================
    log_subsection(logger, "Step 4: Processing All Tranches")
    step_start = time.time()
    
    comprehensive_results, fee_vectors_data_with_dates = calculate_all_tranches(
        fixed_vectors_data, client_claims_data, client_vectors_data,
        fee_vectors_data, valuation_date
    )

    if not comprehensive_results:
        logger.error("No tranches were successfully processed - aborting")
        raise ValueError("No tranches were successfully processed")
    
    logger.info(f"Step 4 completed in {time.time() - step_start:.2f}s")
    log_calculation_result(logger, "Tranches Successfully Processed", len(comprehensive_results))

    # =========================================================================
    # STEP 5: Write Inflation Adjusted Output (if requested)
    # =========================================================================
    if output_both:
        log_subsection(logger, "Step 5: Writing Inflation Adjusted Output")
        step_start = time.time()
        
        inflation_output_file = "data/output/Inflation_Adjusted_Fixed_Vectors.xlsx"
        write_inflation_adjusted_output(comprehensive_results, inflation_output_file)
        
        logger.info(f"Step 5 completed in {time.time() - step_start:.2f}s")
        log_file_operation(logger, "WRITE", inflation_output_file, True)

    # =========================================================================
    # STEP 6: Build Discount Curves
    # =========================================================================
    log_subsection(logger, "Step 6: Building Discount Curves")
    step_start = time.time()
    
    rga_discount_curve = None
    sonia_rates = {}
    if sonia_file_path:
        sonia_rates = load_sonia_rates(sonia_file_path, valuation_date)
        if sonia_rates:
            logger.info(f"Building RGA discount curve with {len(sonia_rates)} SONIA rates")
            logger.debug(f"Discount curve config: spread={DISCOUNT_CURVE_CONFIG['SPREAD']}, "
                        f"sensitivity_shock={DISCOUNT_CURVE_CONFIG['SENSITIVITY_SHOCK']}")
            
            rga_discount_curve = build_rga_discount_curve(
                valuation_date,
                sonia_rates,
                spread=DISCOUNT_CURVE_CONFIG["SPREAD"],
                sensitivity_shock=DISCOUNT_CURVE_CONFIG["SENSITIVITY_SHOCK"]
            )
            log_calculation_result(logger, "RGA Discount Curve Points", len(rga_discount_curve))
        else:
            logger.warning("No SONIA rates loaded - discount curve will be empty")
    else:
        logger.info("No SONIA file provided - skipping discount curve building")
    
    logger.info(f"Step 6 completed in {time.time() - step_start:.2f}s")

    # =========================================================================
    # STEP 7: Extract Client Discount Factors
    # =========================================================================
    log_subsection(logger, "Step 7: Extracting Client Discount Factors")
    step_start = time.time()
    
    client_discount_df = extract_client_discount_factors(client_file_path)
    
    if client_discount_df is not None:
        log_calculation_result(logger, "Client Discount Factor Points", len(client_discount_df))
    else:
        logger.warning("No client discount factors extracted")
    
    logger.info(f"Step 7 completed in {time.time() - step_start:.2f}s")

    # =========================================================================
    # STEP 8: Extract Exposure Summary
    # =========================================================================
    log_subsection(logger, "Step 8: Extracting Client Exposure Summary")
    step_start = time.time()
    
    exposure_summary_data = extract_client_exposure_summary(client_file_path)
    
    log_calculation_result(logger, "Exposure Summary Entries", len(exposure_summary_data))
    logger.info(f"Step 8 completed in {time.time() - step_start:.2f}s")

    # =========================================================================
    # STEP 9: Create Val_Date Output
    # =========================================================================
    log_subsection(logger, "Step 9: Creating Val_Date Output File")
    step_start = time.time()
    
    valdate_output_file = f"data/output/{valuation_date}_Output.xlsx"
    create_valdate_output(
        comprehensive_results,
        rga_discount_curve,
        client_discount_df,
        valuation_date,
        exposure_summary_data,
        fee_pv_data,
        fee_vectors_data_with_dates,
        valdate_output_file
    )
    
    logger.info(f"Step 9 completed in {time.time() - step_start:.2f}s")
    log_file_operation(logger, "WRITE", valdate_output_file, True)

    # =========================================================================
    # STEP 10: Run Sensitivity Scenarios (if requested)
    # =========================================================================
    if run_sensitivity:
        log_subsection(logger, "Step 10: Running Sensitivity Scenarios")
        step_start = time.time()
        
        if not sonia_file_path:
            logger.error("SONIA file path is required for sensitivity runs")
            raise ValueError("SONIA file path is required for sensitivity runs")
        if not prophet_file_path:
            logger.error("Prophet output file path is required for sensitivity runs")
            raise ValueError("Prophet output file path is required for sensitivity runs")

        logger.info("Starting sensitivity scenario calculations...")
        sensitivity_rows = run_sensitivity_runs(
            comprehensive_results=comprehensive_results,
            valuation_date=valuation_date,
            prophet_file_path=prophet_file_path,
            sonia_rates=sonia_rates
        )
        
        sensitivity_output_file = f"data/output/{valuation_date}_CLC_Output.xlsx"
        write_sensitivity_output(sensitivity_rows, sensitivity_output_file)
        
        logger.info(f"Step 10 completed in {time.time() - step_start:.2f}s")
        log_calculation_result(logger, "Sensitivity Result Rows", len(sensitivity_rows))
        log_file_operation(logger, "WRITE", sensitivity_output_file, True)

    # =========================================================================
    # CALCULATION COMPLETE
    # =========================================================================
    total_time = time.time() - calculation_start_time
    
    log_section_end(logger, "Rothesay Collateral Calculation", success=True)
    
    # Summary
    logger.info("=" * 70)
    logger.info("CALCULATION SUMMARY")
    logger.info("=" * 70)
    log_calculation_result(logger, "Total Tranches Processed", len(comprehensive_results))
    log_calculation_result(logger, "Total Execution Time", f"{total_time:.2f}", "seconds")
    logger.info("Output files created:")
    logger.info(f"  - {valdate_output_file}")
    if output_both:
        logger.info("  - data/output/Inflation_Adjusted_Fixed_Vectors.xlsx")
    if run_sensitivity:
        logger.info(f"  - {sensitivity_output_file}")
    logger.info("=" * 70)


def main():
    """Main entry point."""
    gui = CalculatorGUI()
    gui.run()


if __name__ == "__main__":
    main()
