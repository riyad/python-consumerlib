# -*- coding: utf-8 -*-
import pytest

from consumerlib import TimeoutMessage


@pytest.fixture
def timeout_handler():
    return TimeoutMessage(1000, 5)


@pytest.mark.parametrize(
    ('generation', 'ttl'), [
        (0,  1*1000),
        (1,  4*1000),
        (2,  9*1000),
        (3, 16*1000),
        (4, 25*1000),
])
def test_default_message_ttl(timeout_handler, amqp_message, generation, ttl):
    amqp_message['headers']['x-death'] = [
        dict(death=i)
        for i in xrange(generation)
    ]
    next_ttl = timeout_handler.next_message_ttl(amqp_message)

    assert ttl == next_ttl


@pytest.mark.parametrize(
    ('generation', 'ttl'), [
        (0,  1*2000),
        (1,  4*2000),
        (2,  9*2000),
        (3, 16*2000),
        (4, 25*2000),
])
def test_explicit_message_ttl(timeout_handler, amqp_message, generation, ttl):
    amqp_message['headers']['x-death'] = [
        dict(death=i)
        for i in xrange(generation)
    ]
    next_ttl = timeout_handler.next_message_ttl(amqp_message, message_ttl=2000)

    assert ttl == next_ttl
