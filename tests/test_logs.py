import logging
import os

import pytest

from pycrypto import logs  # A importação já executa o dictConfig


def test_logs_directory_creation():
    assert os.path.exists("logs")
    assert os.path.isdir("logs")


def test_logger_configuration():
    root_logger = logging.getLogger("")
    assert root_logger.level == logging.INFO

    ws_logger = logging.getLogger("app.websocket")
    assert ws_logger.level == logging.INFO
    assert ws_logger.propagate is True


def test_log_file_generation():
    logger = logging.getLogger("app.spot")
    test_message = "Teste de cobertura para spot"

    logger.warning(test_message)

    log_file = "logs/spot.log"
    assert os.path.exists(log_file)

    with open(log_file, "r") as f:
        content = f.read()
        assert test_message in content
