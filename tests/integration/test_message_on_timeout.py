# -*- coding: utf-8 -*-
import json

from consumerlib import (setup_consumer, init_safe_message_handler,
    loop, TimeoutMessage, get_death_count)
from consumerlib.tests import (
    break_after, ack_after, fetch_message, Boom, Timer, TEST_DLX)


def test_message_comes_around(puka_client, queue, boom_handler):
    boom_handler.side_effect = [Exception("boom!"), None]
    callback = break_after(
        init_safe_message_handler(
            on_message=ack_after(boom_handler),
            on_error=TimeoutMessage(0, max_deaths=10,
                                    dead_letter_exchange=TEST_DLX),
        ),
        client=puka_client,
        count=2,
    )
    setup_consumer(puka_client, queue, "test:0", callback)
    loop([puka_client])


def test_wait_for_timeout(puka_client, queue, boom_handler):
    with Timer() as t:
        boom_handler.side_effect = [Exception("boom!"), None]
        callback = break_after(
            init_safe_message_handler(
                on_message=ack_after(boom_handler),
                on_error=TimeoutMessage(1100, max_deaths=10,
                                        dead_letter_exchange=TEST_DLX),
            ),
            client=puka_client,
            count=2,
        )
        setup_consumer(puka_client, queue, "test:0", callback)
        loop([puka_client])

    assert t.interval > 1


def test_call_custom_final_death_handler(puka_client, queue, mock_handler, boom_handler):
    callback = break_after(
        init_safe_message_handler(
            on_message=ack_after(boom_handler),
            on_error=TimeoutMessage(0, max_deaths=1,
                                    dead_letter_exchange=TEST_DLX,
                                    on_final_death=ack_after(mock_handler)),
        ),
        client=puka_client,
        count=1,
    )
    setup_consumer(puka_client, queue, "test:0", callback)
    loop([puka_client])

    assert mock_handler.call_count == 1


def test_final_death_handler_arguments(puka_client, queue, mock_handler, boom_handler, message_body):
    timeout_handler = TimeoutMessage(0, max_deaths=1, on_final_death=ack_after(mock_handler))
    callback = break_after(
        init_safe_message_handler(
            on_message=ack_after(boom_handler),
            on_error=timeout_handler,
        ),
        client=puka_client,
        count=1,
    )
    setup_consumer(puka_client, queue, "test:0", callback)
    loop([puka_client])

    client = mock_handler.call_args[0][0]
    message = mock_handler.call_args[0][1]
    exc = mock_handler.call_args[0][2]
    max_deaths = mock_handler.call_args[0][3]
    assert client == puka_client
    assert message['body'] == message_body
    assert isinstance(exc, Boom)
    assert max_deaths == timeout_handler.max_deaths


def test_default_final_death_handlers_sends_message_to_fail_queue(puka_client, queue, boom_handler, failed_messages_queue, message_body):
    callback = break_after(
        init_safe_message_handler(
            on_message=ack_after(boom_handler),
            on_error=TimeoutMessage(0, max_deaths=1),
        ),
        client=puka_client,
        count=1,
    )
    setup_consumer(puka_client, queue, "test:0", callback)
    loop([puka_client])

    message = fetch_message(puka_client, failed_messages_queue)
    assert get_death_count(message) == 1
    assert json.loads(message['body']) == message_body
