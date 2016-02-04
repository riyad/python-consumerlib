#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Usage:
    python -m "examples.consumer" setup
    python -m "examples.consumer" run --message-ttl=1000 --max-deaths=2
"""
import logging

from consumerlib import send_to_fail_queue
from consumerlib.consumer import initialize_timeout_consumer
from consumerlib.setup import setup_queue_with_dlx

logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)


AMQP_URL = 'amqp://guest:guest@127.0.0.1/'
EXCHANGE = 'my_project.topic'
QUEUE = 'foo'
FAILED_MESSAGES_QUEUE = 'failed_messages'


def on_setup(client):
    setup_queue_with_dlx(client, QUEUE, FAILED_MESSAGES_QUEUE,
                         exchange=EXCHANGE)


def on_message(client, message):
    print "received: {}".format(message)
    if 'something bad' in message['body']:
        raise Exception("waah!!")
    print "something useful"
    client.basic_ack(message)


def on_final_death(client, message, exc, max_deaths):
    print "panic!!!"
    #client.basic_ack(message)  # gets rid of the message
    send_to_fail_queue(client, message)  # moves message to fail queue


if __name__ == '__main__':
    consumer = initialize_timeout_consumer(AMQP_URL, QUEUE, on_message,
                                           on_final_death, on_setup=on_setup)
    consumer.main()
