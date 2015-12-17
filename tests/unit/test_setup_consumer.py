# -*- coding: utf-8 -*-
from mock import ANY

from consumerlib import setup_consumer


def test_setting_up_consume_multi(mock_client, noop_handler):
    queue = "test-queue"
    consumer_tag = "test-consumer-tag:0"
    client = setup_consumer(mock_client, queue, consumer_tag, noop_handler)
    client.basic_consume_multi.assert_called_once_with(
            [{'queue': queue, 'consumer_tag': consumer_tag}],
            prefetch_count=ANY, callback=ANY)
