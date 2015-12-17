# -*- coding: utf-8 -*-
from consumerlib import setup_consumer, init_message_handler, loop
from tests import break_after, ack_after


def test_calls_handler(mock_handler, puka_client, queue):
    callback = break_after(
        init_message_handler(ack_after(mock_handler)),
        client=puka_client,
    )
    setup_consumer(puka_client, queue, "test:0", callback)
    loop([puka_client])

    assert mock_handler.call_count == 1


def test_receiving_message(mock_handler, puka_client, queue, message_body):
    callback = break_after(
        init_message_handler(ack_after(mock_handler)),
        client=puka_client,
    )
    setup_consumer(puka_client, queue, "test:0", callback)
    loop([puka_client])

    client = mock_handler.call_args[0][0]
    message = mock_handler.call_args[0][1]
    assert client == puka_client
    assert message['body'] == message_body


def test_message_has_injected_headers(mock_handler, puka_client, queue):
    callback = break_after(
        init_message_handler(ack_after(mock_handler)),
        client=puka_client,
    )
    setup_consumer(puka_client, queue, "test:0", callback)
    loop([puka_client])

    message = mock_handler.call_args[0][1]
    assert 'x-message-id' in message['headers']
    assert message['headers']['x-origin-queue'] == queue
