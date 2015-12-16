# -*- coding: utf-8 -*-
import pytest
import json

from consumerlib.tests import TEST_EXCHANGE, TEST_RK


@pytest.fixture
def amqp_message(message_body, message_headers):
    return {
        "exchange": TEST_EXCHANGE,
        "routing_key": TEST_RK,
        "consumer_tag": "6.0.test:0",
        "headers": message_headers,
        "body": json.dumps(message_body),
    }
