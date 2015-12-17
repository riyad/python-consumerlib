# -*- coding: utf-8 -*-
import logging
from datetime import datetime


TEST_EXCHANGE = 'consumerlib.test.topic'
TEST_DLX = 'consumerlib.test.dlx'
TEST_RK = 'test.consumerlib'


logger = logging.getLogger(__name__)


class Boom(Exception):
    pass


class Timer(object):
    def __enter__(self):
        self.start = datetime.now()
        return self

    def __exit__(self, *args):
        self.end = datetime.now()
        self.interval = (self.end - self.start).total_seconds()


def ack_after(func):
    def _ack_after(client, message, *args):
        func(client, message, *args)
        client.basic_ack(message)
    return _ack_after


def break_after(func, client, count=1):
    class _BreakAfter(object):
        def __init__(self, client, max_count):
            self.client = client
            self.current_count = 0
            self.max_count = max_count

        def __call__(self, *args, **kwargs):
            try:
                self.current_count += 1
                logger.warning("run %s/%s. will break client loop after that",
                               self.current_count, self.max_count)
                func(*args, **kwargs)
            finally:
                if self.current_count >= self.max_count:
                    logger.warning("breaking client loop")
                    self.client.loop_break()
    return _BreakAfter(client, count)


def fetch_message(client, queue):
    return client.wait(client.basic_get(queue, no_ack=True))
