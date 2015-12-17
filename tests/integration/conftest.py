# -*- coding: utf-8 -*-
import json

import pytest

from tests import TEST_EXCHANGE, TEST_RK


@pytest.fixture(autouse=True)
def publish_message(puka_client, message_body, message_headers):
    promise = puka_client.basic_publish(
        exchange=TEST_EXCHANGE,
        routing_key=TEST_RK,
        body=json.dumps(message_body),
        headers=message_headers,
    )
    puka_client.wait(promise)
