# -*- coding: utf-8 -*-
import json
import logging
import select
from functools import partial
from uuid import uuid4

import puka

logger = logging.getLogger(__name__)


DEFAULT_EXCHANGE = 'amq.topic'
DEAD_LETTER_EXCHANGE = 'consumerlib.dlx'

PERSISTENT = 2


class ProtocolError(RuntimeError):
    pass


def count_messages(client, queue):
    result = client.wait(client.queue_declare(queue, passive=True))
    return result['message_count']


def create_queue(client, name, exchange, routing_key,
                 durable=True, auto_delete=False):
    client.wait(client.queue_declare(
        name, durable=durable, auto_delete=auto_delete))
    client.wait(client.queue_bind(name, exchange, routing_key))


def get_death_count(message):
    return sum(death.get('count', 1)
               for death in message['headers'].get('x-death', []))


def get_filtered_headers(message):
    blacklisted_headers = ('x-puka-delivery-tag',)
    return {
        k: v for k, v in message['headers'].iteritems()
        if k not in blacklisted_headers
    }


def get_message_id(message):
    return message['headers']['x-message-id']


def get_origin_queue(message):
    return message['headers']['x-origin-queue']


def init_message_handler(on_message):
    def handle_message(client, queue, promise, message):
        logger.info("handling message: %s", message)
        _handle_protocol_error(message)

        if _inject_missing_headers(client, message, queue):
            return

        message['body'] = _body_from_json(message)
        logger.debug("calling on_message handler with: %s", message)
        on_message(client, message)
    return handle_message


def init_safe_message_handler(on_message, on_error):
    def handle_message_safe(client, queue, promise, message):
        logger.info("handling message: %s", message)
        _handle_protocol_error(message)

        if _inject_missing_headers(client, message, queue):
            return

        message_id = get_message_id(message)
        message['body'] = _body_from_json(message)
        try:
            logger.debug("calling on_message handler with: %s", message)
            on_message(client, message)
        except Exception as exc:
            logger.exception("caught unhandled exception in on_message "
                             "handler for %s", message_id)
            logger.debug("calling on_error handler with: %s", message)
            on_error(client, message, exc)
    return handle_message_safe


def init_simple_handler(on_message):
    def handle_message(client, queue, promise, message):
        logger.info("handling message: %s", message)
        _handle_protocol_error(message)

        message['body'] = _body_from_json(message)
        logger.debug("calling on_message handler with: %s", message)
        on_message(client, message)
    return handle_message


def republish(client, message):
    message_id = get_message_id(message)
    queue = get_origin_queue(message)
    logger.debug("republishing %s to %s", message_id, queue)
    promise = client.basic_publish(
        exchange='',
        routing_key=queue,
        body=_body_to_json(message),
        headers=get_filtered_headers(message),
    )
    client.wait(promise)


class TimeoutMessage(object):
    """
    In contrast to publishing a message dead lettering doesn't route to the
    queue (as specified in the dead letter routing key) when the
    dead letter exchange is "".

    For that reason there's the explicit "dead_letter_exchange" argument and a
    binding from the time-out-queue through the "dead_letter_exchange" to the
    origin queue.
    """

    def __init__(self, message_ttl, max_deaths,
                 queue_ttl_extra=1000,
                 dead_letter_exchange=DEAD_LETTER_EXCHANGE,
                 on_final_death=None):
        self.message_ttl = message_ttl
        self.max_deaths = max_deaths
        self.queue_ttl_extra = queue_ttl_extra
        self.dead_letter_exchange = dead_letter_exchange
        self.on_final_death = on_final_death or send_to_fail_queue

    def next_message_ttl(self, message, message_ttl=None):
        """
        In the best case (immediate consumption after requeue) the message will
        have been on timeout for a total of 1, 5, 14, 30, 55, 91, 140, 204,
        285, 670, etc. times the original timeout.
        """
        ttl = message_ttl or self.message_ttl
        x = get_death_count(message) + 1
        return x * x * ttl

    def __call__(self, client, message, exc):
        message_id = get_message_id(message)
        if _death_limit_reached(self.max_deaths-1, message):
            self.on_final_death(client, message, exc, self.max_deaths)
            return

        new_message_ttl = self.next_message_ttl(message, self.message_ttl)
        time_out_queue = self._timeout_queue_name(message)
        logger.info(
            "putting %s on timeout into %s for %sms",
            message_id, time_out_queue, new_message_ttl)

        client.queue_declare(
            time_out_queue,
            arguments={
                'x-expires': new_message_ttl + self.queue_ttl_extra,
                'x-message-ttl': new_message_ttl,
                'x-dead-letter-exchange': self.dead_letter_exchange,
                'x-dead-letter-routing-key': get_origin_queue(message),
            },
            callback=partial(self._on_queue_declared, client, message)
        )

    def _on_queue_declared(self, client, message, promise, result):
        client.basic_publish(
            exchange='',
            routing_key=self._timeout_queue_name(message),
            body=_body_to_json(message),
            headers=get_filtered_headers(message),
            callback=partial(self._on_message_published, client, message)
        )

    def _on_message_published(self, client, message, promise, result):
        client.basic_ack(message)

    def _timeout_queue_name(self, message):
        return "toq-gen{}-{}-{}".format(
            get_death_count(message),
            get_origin_queue(message),
            get_message_id(message),
        )


def loop(clients, interactive=False):
    """
    puka's loop(clients) does not handle clients' loop_break, this version
    does.
    """
    # reset loop_break for all clients
    for client in clients:
        client._loop_break = False

    while len(clients):
        for client in clients:
            client.run_any_callbacks()

        readers = clients
        writers = [client for client in clients if client.needs_write()]
        try:
            read_clients, write_clients, error_clients =\
                select.select(readers, writers, readers)
        except KeyboardInterrupt:
            if interactive:
                for client_ in clients:
                    client_.loop_break()
            else:
                raise

        for client in read_clients:
            client.on_read()

        for client in error_clients:
            client.on_read()

        for client in write_clients:
            client.on_write()

        for client in clients:
            if getattr(client, '_loop_break', False):
                if client.needs_write():
                    logger.warn("breaking client that still needs to write :/")
                    client.on_write()
                clients.remove(client)


def send_to_fail_queue(client, message, exc, max_deaths=None):
    if max_deaths is None:
        logger.info("%s  will be sent to fail queue", get_message_id(message))
    else:
        logger.info("%s reached death limit %s, will be sent to fail queue",
                    get_message_id(message), max_deaths)
    client.basic_reject(message, requeue=False)


def setup_consumer(client, queue, consumer_tag, callback):
    logger.debug("setting up consumer")
    client.basic_consume_multi(
        [dict(queue=queue, consumer_tag=consumer_tag)],
        prefetch_count=1,
        callback=partial(callback, client, queue),
    )
    return client


def setup_consumer_client(url):
    logger.debug("connecting consumer client")
    consumer_client = puka.Client(url)
    consumer_client.wait(consumer_client.connect())
    return consumer_client


def _body_from_json(message):
    if not isinstance(message['body'], dict):
        return json.loads(message['body'])
    return message['body']


def _body_to_json(message):
    if isinstance(message['body'], dict):
        return json.dumps(message['body'])
    return message['body']


def _death_limit_reached(max_deaths, message):
    death_count = get_death_count(message)
    return death_count >= max_deaths


def _handle_protocol_error(message):
    if isinstance(message, puka.spec.FrameChannelClose):
        raise ProtocolError(message['reply_text'])


def _inject_missing_headers(client, message, queue):
    injected = any([
        _set_message_header(message, 'x-message-id', str(uuid4())),
        _set_message_header(message, 'x-origin-queue', queue),
    ])
    if injected:
        republish(client, message)
        client.basic_ack(message)
    return injected


def _set_message_header(message, name, value):
    if name not in message['headers']:
        logger.debug("message header %s missing", name)
        message['headers'][name] = value
        return True
