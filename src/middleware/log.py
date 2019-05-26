import logging
import os

def config_log(proccess_name):
    logging.basicConfig(level=logging.INFO)
    logFormatter = logging.Formatter(proccess_name+" %(asctime)s - [%(process)d] - %(message)s")
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler("{}.log".format(os.environ['LOGS_FILE']))
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
