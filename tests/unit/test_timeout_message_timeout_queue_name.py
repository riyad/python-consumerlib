# -*- coding: utf-8 -*-
import pytest

from consumerlib import TimeoutMessage, get_message_id, get_origin_queue


@pytest.fixture
def timeout_handler():
    return TimeoutMessage(1000, 5)


@pytest.mark.parametrize('generation', range(5))
def test_generation_in_toq_name(timeout_handler, amqp_message, generation):
    amqp_message['headers']['x-death'] = [
        dict(death=i)
        for i in xrange(generation)
    ]
    toq_name = timeout_handler._timeout_queue_name(amqp_message)
    assert "gen{}".format(generation) in toq_name


def test_origin_queue_in_toq_name(timeout_handler, amqp_message):
    toq_name = timeout_handler._timeout_queue_name(amqp_message)
    origin_queue = get_origin_queue(amqp_message)
    assert origin_queue in toq_name


def test_message_id_in_toq_name(timeout_handler, amqp_message):
    toq_name = timeout_handler._timeout_queue_name(amqp_message)
    message_id = get_message_id(amqp_message)
    assert message_id in toq_name
