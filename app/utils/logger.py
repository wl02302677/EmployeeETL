import logging.handlers


class Logger(object):
    # Disable matplotlib debug messages
    # logging.getLogger('matplotlib').setLevel(logging.WARNING)
    logging.getLogger('matplotlib').setLevel(logging.DEBUG)

    logger = logging.getLogger(__name__)

    @classmethod
    def info(cls, message):
        cls.logger.info("{}".format(message))

    @classmethod
    def warn(cls, message):
        cls.logger.warn("{}".format(message))

    @classmethod
    def debug(cls, message):
        cls.logger.debug("{}".format(message))

    @classmethod
    def error(cls, message):
        cls.logger.error("{}".format(message))
