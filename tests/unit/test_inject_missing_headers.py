# -*- coding: utf-8 -*-
from mock import patch
import pytest
from consumerlib import _inject_missing_headers


@pytest.fixture
def message(message_body):
    return dict(
        headers=dict(),
        body=message_body,
    )


@pytest.fixture
def full_message(message, message_headers):
    message['headers'] = message_headers
    return message


def test_injects_x_message_id(mock_client, full_message, queue):
    del full_message['headers']['x-message-id']
    injected = _inject_missing_headers(mock_client, full_message, queue)
    assert injected
    assert 'x-message-id' in full_message['headers']


def test_injects_x_origin_queue(mock_client, full_message, queue):
    del full_message['headers']['x-origin-queue']
    injected = _inject_missing_headers(mock_client, full_message, queue)
    assert injected
    assert 'x-origin-queue' in full_message['headers']
    assert full_message['headers']['x-origin-queue'] == queue


def test_doesnt_change_body(mock_client, message, queue, message_body):
    _inject_missing_headers(mock_client, message, queue)
    assert message['body'] == message_body


@patch("consumerlib.republish")
def test_republish_after_inject(_republish, mock_client, message, queue):
    _inject_missing_headers(mock_client, message, queue)

    _republish.assert_called_once_with(mock_client, message)


def test_ack_after_republish(mock_client, message, queue):
    _inject_missing_headers(mock_client, message, queue)

    mock_client.basic_ack.assert_called_once_with(message)


def test_doesnt_inject_with_full_headers(mock_client, full_message, queue):
    injected = _inject_missing_headers(mock_client, full_message, queue)
    assert not injected


def test_doesnt_republish_with_full_headers(mock_client, full_message, queue):
    _inject_missing_headers(mock_client, full_message, queue)
    assert mock_client.basic_publish.call_count == 0
    assert mock_client.basic_ack.call_count == 0
