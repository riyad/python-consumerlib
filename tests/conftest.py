# -*- coding: utf-8 -*-
import json
from mock import MagicMock
from uuid import uuid4

import puka
import pytest

from consumerlib import (
    init_safe_message_handler, setup_consumer, TimeoutMessage)
from tests import Boom, break_after, TEST_EXCHANGE, TEST_DLX, TEST_RK
from consumerlib.setup import setup_queue, setup_queue_with_dlx


@pytest.fixture
def test_name(request):
    return request.node.nodeid.split('/')[-1]


@pytest.fixture
def amqp_url():
    return 'amqp://guest:guest@127.0.0.1/'


@pytest.fixture
def queue(test_name):
    return "_{}_{}".format(test_name, uuid4())


@pytest.fixture
def failed_messages_queue(queue):
    return "_failed_messages{}".format(queue)


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def puka_client(puka_client_with_catchall_queue):
    return puka_client_with_catchall_queue


@pytest.fixture
def puka_client_basic(request, amqp_url):
    client = puka.Client(amqp_url)
    client.wait(client.connect())
    def _close_client():
        client.wait(client.close())
    request.addfinalizer(_close_client)
    return client


@pytest.fixture
def puka_client_with_simple_queue(puka_client_basic, request, queue):
    setup_queue(puka_client_basic, queue, exchange=TEST_EXCHANGE, durable=False)
    def _delete_queue():
        puka_client_basic.wait(puka_client_basic.queue_delete(queue))
    request.addfinalizer(_delete_queue)
    return puka_client_basic


@pytest.fixture
def puka_client_with_catchall_queue(puka_client_basic, request, queue,
                                    failed_messages_queue):
    setup_queue_with_dlx(puka_client_basic, queue, failed_messages_queue,
                         exchange=TEST_EXCHANGE, dlx=TEST_DLX, durable=False)
    def _delete_queues():
        puka_client_basic.wait(puka_client_basic.queue_delete(queue))
        puka_client_basic.wait(
                puka_client_basic.queue_delete(failed_messages_queue))
    request.addfinalizer(_delete_queues)
    return puka_client_basic


@pytest.fixture
def consumer_client(puka_client_with_catchall_queue, queue, message_handler,
                    error_handler, max_consume, test_name):
    _client = puka_client_with_catchall_queue
    callback = break_after(
        init_safe_message_handler(
            on_message=message_handler,
            on_error=error_handler,
        ),
        client=_client,
        count=max_consume)
    consumer_tag = '{}:0'.format(test_name)
    setup_consumer(_client, queue, consumer_tag, callback)
    return _client


@pytest.fixture
def message_headers(queue):
    return {
        'x-message-id': str(uuid4()),
        'x-origin-queue': queue,
    }


@pytest.fixture
def boom_handler():
    handler = MagicMock(__name__="boom_handler")
    handler.side_effect = Boom("booom!!")
    return handler


@pytest.fixture
def noop_error_handler(max_retry):
    return TimeoutMessage(0, max_deaths=max_retry,
                          dead_letter_exchange=TEST_DLX)


@pytest.fixture
def mock_handler():
    return MagicMock(__name__="mock_handler")


@pytest.fixture
def noop_handler():
    return lambda *args: None


@pytest.fixture
def max_consume():
    return 1


@pytest.fixture
def max_retry():
    return 5


@pytest.fixture
def publish(consumer_client, message_headers):
    def _publish_inner(body):
        promise = consumer_client.basic_publish(
            exchange=TEST_EXCHANGE,
            routing_key=TEST_RK,
            body=json.dumps(body),
            headers=message_headers)
        consumer_client.wait(promise)
    return _publish_inner


@pytest.fixture
def message_body(request):
    test_name = request.node.nodeid.split('/')[-1]
    return dict(
        foo="bar",
        test_name=test_name,
    )
