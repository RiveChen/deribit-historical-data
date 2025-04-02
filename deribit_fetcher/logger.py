import logging
from tqdm import tqdm


class TqdmLoggingHandler(logging.Handler):
    """Custom logging handler that integrates with tqdm progress bars.
    Prevents log messages from interfering with progress bar display.
    """

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


# Initialize the global logger with NullHandler to avoid "No handlers" warnings
_global_logger = logging.getLogger(__name__)
_global_logger.addHandler(logging.NullHandler())


def configure_global_logger(verbose=False, log_file=None, logger_name=__name__):
    """Configure the global logger for the package.

    Args:
        verbose (bool): If True, sets log level to INFO. Otherwise ERROR.
        log_file (str): Path to log file. If None, no file logging.
        logger_name (str): Name for the logger hierarchy.
    """
    global _global_logger

    # Get or create logger
    _global_logger = logging.getLogger(logger_name)

    # Clear existing handlers to avoid duplicates
    for handler in _global_logger.handlers[:]:
        _global_logger.removeHandler(handler)

    # Set log level
    log_level = logging.INFO if verbose else logging.ERROR
    _global_logger.setLevel(log_level)

    # Create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Add console handler with tqdm integration
    console_handler = TqdmLoggingHandler()
    console_handler.setFormatter(formatter)
    _global_logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        _global_logger.addHandler(file_handler)

    # Prevent propagation to root logger to avoid duplicate logs
    _global_logger.propagate = False

    return _global_logger


def get_global_logger():
    """Get the configured global logger instance."""
    return _global_logger
