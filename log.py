import config
import logging


class LogFormatter(logging.Formatter):
    err_fmt = "%(asctime)s %(levelname)s: [%(filename)s::%(funcName)s()::%(lineno)s] %(message)s"
    dbg_fmt = "%(asctime)s %(levelname)s: [%(filename)s::%(funcName)s()::%(lineno)s] %(message)s"
    info_fmt = "%(asctime)s %(levelname)s: [%(filename)s] %(message)s"

    def __init__(self):
        super().__init__(fmt="%(levelno)d: %(msg)s", datefmt=None, style="%")

    def format(self, record):
        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._style._fmt

        # Replace the original format with one customized by logging level
        if record.levelno == logging.DEBUG:
            self._style._fmt = self.dbg_fmt

        elif record.levelno == logging.INFO:
            self._style._fmt = self.info_fmt

        elif record.levelno == logging.ERROR:
            self._style._fmt = self.err_fmt

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._style._fmt = format_orig

        return result


def get_logger(name="logger_obj", level="INFO", mode="w"):
    logger_obj = logging.getLogger(name)

    fh = logging.FileHandler(name + ".log", mode)
    ch = logging.StreamHandler()

    if level == "INFO":
        logger_obj.setLevel(logging.INFO)
        fh.setLevel(logging.INFO)
        ch.setLevel(logging.INFO)
    if level == "DEBUG":
        logger_obj.setLevel(logging.DEBUG)
        fh.setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    if level == "ERROR":
        logger_obj.setLevel(logging.ERROR)
        fh.setLevel(logging.ERROR)
        ch.setLevel(logging.ERROR)

    formatter = LogFormatter()
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger_obj.addHandler(fh)
    logger_obj.addHandler(ch)

    return logger_obj


logger = get_logger(name=config.LOGGER_NAME, level=config.LOG_LEVEL)
