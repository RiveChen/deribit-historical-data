import logging
from tqdm import tqdm


class TqdmLoggingHandler(logging.Handler):
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            msg = self.format(record)
            # Use tqdm.write instead of print or stream.write
            # tqdm.write will automatically pause the progress bar, print the message, and then redraw the progress bar
            tqdm.write(msg)
            self.flush()
        except Exception:
            self.handleError(record)


def setup_logging():
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
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
