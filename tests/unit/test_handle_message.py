# -*- coding: utf-8 -*-
from mock import patch

import puka
import pytest

from consumerlib import init_message_handler, ProtocolError
from tests import Boom


def test_calls_handler(mock_handler, mock_client, queue, amqp_message):
    init_message_handler(mock_handler)(mock_client, queue, 0, amqp_message)
    assert mock_handler.call_count == 1


def test_handler_arguments(mock_handler, mock_client, queue, amqp_message):
    init_message_handler(mock_handler)(mock_client, queue, 0, amqp_message)

    mock_handler.assert_called_once_with(mock_client, amqp_message)


def test_raises_protocol_error(mock_handler, mock_client, queue):
    message = puka.spec.FrameChannelClose(reply_text="boom!")
    with pytest.raises(ProtocolError):
        init_message_handler(mock_handler)(mock_client, queue, 0, message)


def test_inject_message_id_header_if_missing(noop_handler, mock_client, queue, amqp_message):
    message_id_before = amqp_message['headers']['x-message-id']
    del amqp_message['headers']['x-message-id']
    init_message_handler(noop_handler)(mock_client, queue, 0, amqp_message)
    assert amqp_message['headers']['x-message-id'] != message_id_before


@patch("consumerlib.republish")
def test_republish_if_message_id_header_missing(_republish, noop_handler, mock_client, queue, amqp_message):
    del amqp_message['headers']['x-message-id']
    init_message_handler(noop_handler)(mock_client, queue, 0, amqp_message)

    _republish.assert_called_once_with(mock_client, amqp_message)


def test_dont_overwrite_message_id_header(noop_handler, mock_client, queue, amqp_message):
    message_id_before = amqp_message['headers']['x-message-id']
    init_message_handler(noop_handler)(mock_client, queue, 0, amqp_message)
    assert amqp_message['headers']['x-message-id'] == message_id_before


def test_inject_origin_queue_header_if_missing(noop_handler, mock_client, queue, amqp_message):
    del amqp_message['headers']['x-origin-queue']
    init_message_handler(noop_handler)(mock_client, queue, 0, amqp_message)
    assert amqp_message['headers']['x-origin-queue'] == queue


def test_parse_message_body_as_json(mock_handler, mock_client, queue, amqp_message):
    init_message_handler(mock_handler)(mock_client, queue, 0, amqp_message)

    handler_message = mock_handler.call_args[0][1]
    assert isinstance(handler_message['body'], dict)


def test_dont_ack_message_on_success(noop_handler, mock_client, queue, amqp_message):
    init_message_handler(noop_handler)(mock_client, queue, 0, amqp_message)
    assert mock_client.basic_ack.call_count == 0


def test_dont_ack_message_on_error(boom_handler, mock_client, queue, amqp_message):
    with pytest.raises(Boom):
        init_message_handler(boom_handler)(mock_client, queue, 0, amqp_message)
    assert mock_client.basic_ack.call_count == 0


def test_dont_catch_exception(boom_handler, mock_client, queue, amqp_message):
    with pytest.raises(Boom) as exc:
        init_message_handler(boom_handler)(mock_client, queue, 0, amqp_message)
    assert exc.value.message == "booom!!"
