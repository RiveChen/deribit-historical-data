import logging
from tqdm import tqdm


class TqdmLoggingHandler(logging.Handler):
    """
    Logging handler compatible with tqdm progress bars.

    Uses tqdm.write() to print log messages, which temporarily pauses the
    progress bar, prints the message, and redraws the bar — avoiding visual
    corruption of progress output.
    """

    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging():
    """Configure root logger with tqdm-compatible output and structured formatting."""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Suppress noisy httpx debug logging
    logging.getLogger("httpx").setLevel(logging.WARNING)

    if root_logger.handlers:
        root_logger.handlers = []

    handler = TqdmLoggingHandler()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    root_logger.addHandler(handler)
