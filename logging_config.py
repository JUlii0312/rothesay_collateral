"""
LOGGING CONFIGURATION MODULE
============================
Centralized logging setup for the UK Longevity Swap Calculator.

Provides:
- Structured logging with timestamps
- Console and file output
- Progress tracking utilities
- Performance timing decorators
"""

import logging
import os
import sys
from datetime import datetime
from functools import wraps
from typing import Optional, Any, Callable
import time


# =============================================================================
# Logging Configuration
# =============================================================================

# Log format with timestamp, level, module, and message
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Default log level
DEFAULT_LOG_LEVEL = logging.INFO


def setup_logging(
    log_level: int = DEFAULT_LOG_LEVEL,
    log_file: Optional[str] = None,
    log_dir: str = "data/output/logs"
) -> logging.Logger:
    """
    Configure and return the root logger for the application.
    
    Args:
        log_level: Logging level (e.g., logging.DEBUG, logging.INFO)
        log_file: Optional specific log file name
        log_dir: Directory for log files
    
    Returns:
        Configured root logger
    """
    # Create logger
    logger = logging.getLogger("sensitivity")
    logger.setLevel(log_level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    
    # Console handler with color support
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(ColoredFormatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_file or log_dir:
        # Ensure log directory exists
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        # Generate log file name if not provided
        if not log_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = os.path.join(log_dir, f"calculation_{timestamp}.log")
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        logger.info(f"Log file: {log_file}")
    
    return logger


class ColoredFormatter(logging.Formatter):
    """Custom formatter with color support for console output."""
    
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m',      # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        # Add color to level name
        level_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        # Store original levelname
        original_levelname = record.levelname
        
        # Apply color
        record.levelname = f"{level_color}{record.levelname}{reset}"
        
        # Format the message
        result = super().format(record)
        
        # Restore original levelname
        record.levelname = original_levelname
        
        return result


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Module name (typically __name__)
    
    Returns:
        Logger instance
    """
    return logging.getLogger(f"sensitivity.{name}")


# =============================================================================
# Progress Tracking Utilities
# =============================================================================

class ProgressTracker:
    """
    Track progress of iterative operations with logging.
    
    Usage:
        tracker = ProgressTracker(total=100, name="Processing tranches")
        for item in items:
            process(item)
            tracker.update()
        tracker.complete()
    """
    
    def __init__(
        self,
        total: int,
        name: str = "Progress",
        log_interval: int = 10,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize progress tracker.
        
        Args:
            total: Total number of items to process
            name: Name of the operation
            log_interval: Percentage interval for progress logging
            logger: Logger instance (uses default if not provided)
        """
        self.total = total
        self.name = name
        self.log_interval = log_interval
        self.logger = logger or get_logger("progress")
        
        self.current = 0
        self.start_time = time.time()
        self.last_logged_percent = 0
        
        self.logger.info(f"Starting: {name} ({total} items)")
    
    def update(self, increment: int = 1, message: Optional[str] = None) -> None:
        """
        Update progress counter.
        
        Args:
            increment: Number of items completed
            message: Optional message to log with progress
        """
        self.current += increment
        
        if self.total > 0:
            percent = int((self.current / self.total) * 100)
            
            # Log at intervals
            if percent >= self.last_logged_percent + self.log_interval:
                elapsed = time.time() - self.start_time
                rate = self.current / elapsed if elapsed > 0 else 0
                eta = (self.total - self.current) / rate if rate > 0 else 0
                
                msg = f"{self.name}: {percent}% ({self.current}/{self.total})"
                if message:
                    msg += f" - {message}"
                msg += f" [Rate: {rate:.1f}/s, ETA: {eta:.1f}s]"
                
                self.logger.info(msg)
                self.last_logged_percent = percent
    
    def complete(self, message: Optional[str] = None) -> None:
        """
        Mark operation as complete and log summary.
        
        Args:
            message: Optional completion message
        """
        elapsed = time.time() - self.start_time
        rate = self.current / elapsed if elapsed > 0 else 0
        
        msg = f"Completed: {self.name} ({self.current} items in {elapsed:.2f}s, {rate:.1f}/s)"
        if message:
            msg += f" - {message}"
        
        self.logger.info(msg)


# =============================================================================
# Timing Decorators
# =============================================================================

def log_execution_time(func: Callable) -> Callable:
    """
    Decorator to log function execution time.
    
    Usage:
        @log_execution_time
        def my_function():
            ...
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        logger = get_logger(func.__module__.split('.')[-1] if func.__module__ else "main")
        
        start_time = time.time()
        logger.debug(f"Starting: {func.__name__}()")
        
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time
            logger.info(f"Completed: {func.__name__}() in {elapsed:.3f}s")
            return result
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Failed: {func.__name__}() after {elapsed:.3f}s - {type(e).__name__}: {e}")
            raise
    
    return wrapper


def log_step(step_name: str):
    """
    Decorator to log a named step with timing.
    
    Usage:
        @log_step("Loading SONIA rates")
        def load_sonia():
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = get_logger(func.__module__.split('.')[-1] if func.__module__ else "main")
            
            start_time = time.time()
            logger.info(f"[STEP] {step_name} - Starting...")
            
            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - start_time
                logger.info(f"[STEP] {step_name} - Completed in {elapsed:.3f}s")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"[STEP] {step_name} - Failed after {elapsed:.3f}s: {type(e).__name__}: {e}")
                raise
        
        return wrapper
    return decorator


# =============================================================================
# Data Logging Utilities
# =============================================================================

def log_dataframe_info(
    logger: logging.Logger,
    df: Any,  # pd.DataFrame
    name: str,
    level: int = logging.DEBUG
) -> None:
    """
    Log summary information about a DataFrame.
    
    Args:
        logger: Logger instance
        df: Pandas DataFrame
        name: Name/description of the DataFrame
        level: Logging level
    """
    import pandas as pd
    
    if not isinstance(df, pd.DataFrame):
        logger.log(level, f"{name}: Not a DataFrame (type: {type(df).__name__})")
        return
    
    if df.empty:
        logger.log(level, f"{name}: Empty DataFrame")
        return
    
    msg = (
        f"{name}: {len(df)} rows x {len(df.columns)} columns | "
        f"Columns: {list(df.columns)[:5]}{'...' if len(df.columns) > 5 else ''} | "
        f"Memory: {df.memory_usage(deep=True).sum() / 1024:.1f} KB"
    )
    logger.log(level, msg)


def log_dict_summary(
    logger: logging.Logger,
    data: dict,
    name: str,
    level: int = logging.DEBUG
) -> None:
    """
    Log summary information about a dictionary.
    
    Args:
        logger: Logger instance
        data: Dictionary to summarize
        name: Name/description of the dictionary
        level: Logging level
    """
    if not data:
        logger.log(level, f"{name}: Empty dictionary")
        return
    
    keys_preview = list(data.keys())[:5]
    msg = (
        f"{name}: {len(data)} entries | "
        f"Keys: {keys_preview}{'...' if len(data) > 5 else ''}"
    )
    logger.log(level, msg)


def log_calculation_result(
    logger: logging.Logger,
    result_name: str,
    value: Any,
    unit: str = "",
    precision: int = 4
) -> None:
    """
    Log a calculation result with formatting.
    
    Args:
        logger: Logger instance
        result_name: Name of the calculated value
        value: The calculated value
        unit: Optional unit string
        precision: Decimal precision for floats
    """
    if isinstance(value, float):
        formatted_value = f"{value:,.{precision}f}"
    elif isinstance(value, int):
        formatted_value = f"{value:,}"
    else:
        formatted_value = str(value)
    
    msg = f"[RESULT] {result_name}: {formatted_value}"
    if unit:
        msg += f" {unit}"
    
    logger.info(msg)


# =============================================================================
# Section/Phase Logging
# =============================================================================

def log_section_start(logger: logging.Logger, section_name: str) -> None:
    """Log the start of a major section/phase."""
    separator = "=" * 70
    logger.info(separator)
    logger.info(f"STARTING: {section_name}")
    logger.info(separator)


def log_section_end(logger: logging.Logger, section_name: str, success: bool = True) -> None:
    """Log the end of a major section/phase."""
    separator = "-" * 70
    status = "SUCCESS" if success else "FAILED"
    logger.info(separator)
    logger.info(f"COMPLETED: {section_name} - {status}")
    logger.info(separator)


def log_subsection(logger: logging.Logger, subsection_name: str) -> None:
    """Log the start of a subsection."""
    logger.info(f"--- {subsection_name} ---")


# =============================================================================
# Validation and Error Logging
# =============================================================================

def log_validation_result(
    logger: logging.Logger,
    validation_name: str,
    is_valid: bool,
    details: Optional[str] = None
) -> None:
    """
    Log a validation check result.
    
    Args:
        logger: Logger instance
        validation_name: Name of the validation check
        is_valid: Whether validation passed
        details: Optional details message
    """
    if is_valid:
        msg = f"[VALIDATION PASS] {validation_name}"
        if details:
            msg += f": {details}"
        logger.debug(msg)
    else:
        msg = f"[VALIDATION FAIL] {validation_name}"
        if details:
            msg += f": {details}"
        logger.warning(msg)


def log_file_operation(
    logger: logging.Logger,
    operation: str,
    file_path: str,
    success: bool = True,
    details: Optional[str] = None
) -> None:
    """
    Log a file operation (read/write/create).
    
    Args:
        logger: Logger instance
        operation: Operation type (e.g., "READ", "WRITE", "CREATE")
        file_path: Path to the file
        success: Whether operation succeeded
        details: Optional details
    """
    status = "OK" if success else "FAILED"
    msg = f"[FILE {operation}] {file_path} - {status}"
    if details:
        msg += f" ({details})"
    
    if success:
        logger.info(msg)
    else:
        logger.error(msg)


# =============================================================================
# Initialize default logger
# =============================================================================

# Create a default logger that can be used before setup_logging is called
_default_logger = logging.getLogger("sensitivity")
_default_logger.setLevel(logging.INFO)

if not _default_logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    _default_logger.addHandler(_handler)
