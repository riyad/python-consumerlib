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
