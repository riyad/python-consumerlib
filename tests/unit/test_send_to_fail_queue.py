# -*- coding: utf-8 -*-
from mock import MagicMock

import pytest

from consumerlib import send_to_fail_queue


@pytest.fixture
def exc():
    return MagicMock()


@pytest.fixture
def max_deaths():
    return 5


def test_can_be_used_on_error(mock_client, amqp_message, exc):
    send_to_fail_queue(mock_client, amqp_message, exc)


def test_rejects_message_on_error(mock_client, amqp_message, exc):
    send_to_fail_queue(mock_client, amqp_message, exc)
    mock_client.basic_reject.assert_called_once_with(amqp_message, requeue=False)


def test_can_be_used_on_final_death(mock_client, amqp_message, exc, max_deaths):
    send_to_fail_queue(mock_client, amqp_message, exc, max_deaths)


def test_rejects_message_on_final_death(mock_client, amqp_message, exc, max_deaths):
    send_to_fail_queue(mock_client, amqp_message, exc, max_deaths)
    mock_client.basic_reject.assert_called_once_with(amqp_message, requeue=False)
