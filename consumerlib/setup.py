# -*- coding: utf-8 -*-
from consumerlib import DEFAULT_EXCHANGE, DEAD_LETTER_EXCHANGE


def setup_queue(client, queue, exchange=DEFAULT_EXCHANGE, routing_key='#',
                durable=True):
    client.wait(client.exchange_declare(
        exchange,
        type='topic',
        durable=durable
    ))
    client.wait(client.queue_declare(queue, durable=durable))
    client.wait(client.queue_bind(queue, exchange, routing_key))


def setup_queue_with_dlx(client, queue, failed_messages_queue,
                         exchange=DEFAULT_EXCHANGE, routing_key='#',
                         dlx=DEAD_LETTER_EXCHANGE, durable=True):
    client.wait(client.exchange_declare(
        exchange,
        type='topic',
        durable=durable
    ))
    client.wait(client.exchange_declare(
        dlx,
        type='topic',
        durable=durable
    ))
    client.wait(client.queue_declare(
        queue,
        arguments={
            'x-dead-letter-exchange': dlx,
            'x-dead-letter-routing-key': failed_messages_queue,
        },
        durable=durable,
    ))
    client.wait(client.queue_bind(queue, exchange, routing_key))
    client.wait(client.queue_bind(queue, dlx, queue))
    client.wait(client.queue_declare(
        failed_messages_queue,
        durable=durable,
    ))
    client.wait(client.queue_bind(failed_messages_queue, dlx,
                                  failed_messages_queue))
