# -*- coding: utf-8 -*-
import json
from mock import MagicMock, ANY

import pytest

from consumerlib import republish, get_origin_queue, filter_headers


@pytest.fixture
def amqp_message(amqp_message):
    amqp_message['body'] = json.loads(amqp_message['body'])
    return amqp_message


def test_publish_to_origin_queue(mock_client, amqp_message):
    republish(mock_client, amqp_message)

    mock_client.basic_publish.assert_called_once_with(
        exchange='',
        routing_key=get_origin_queue(amqp_message),
        body=ANY,
        headers=ANY,
    )


def test_filters_headers(mock_client, amqp_message):
    amqp_message['headers']['x-puka-delivery-tag'] = "foo"
    republish(mock_client, amqp_message)

    mock_client.basic_publish.assert_called_once_with(
        exchange=ANY,
        routing_key=ANY,
        body=ANY,
        headers=filter_headers(amqp_message['headers']),
    )
    assert 'x-puka-delivery-tag' not in mock_client.basic_publish.call_args[1]['headers']


def test_converts_body_to_json(mock_client, amqp_message):
    body = amqp_message['body']
    assert isinstance(body, dict)
    republish(mock_client, amqp_message)

    mock_client.basic_publish.assert_called_once_with(
        exchange=ANY,
        routing_key=ANY,
        body=json.dumps(body),
        headers=ANY,
    )


def test_waits_for_publish(mock_client, amqp_message):
    promise = MagicMock()
    mock_client.basic_publish.return_value = promise
    republish(mock_client, amqp_message)

    mock_client.wait.assert_called_once_with(promise)
