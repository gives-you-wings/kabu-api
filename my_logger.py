from logging import getLogger, basicConfig, INFO


class MyLogger:
    def __init__(self, name):
        print('init my_logger.py')
        self.logger = getLogger(name)
        logging_format = ("%(asctime)s\t%(levelname)s\t%(threadName)s\t%(name)s\t%(filename)s\t%(funcName)s\t%(lineno)d"
                          "\t%(message)s")
        basicConfig(level=INFO, format=logging_format)

    def info(self, message):
        self.logger.info(message)
