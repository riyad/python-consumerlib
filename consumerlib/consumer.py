# -*- coding: utf-8 -*-
import inspect
from functools import partial

import click

from . import (init_safe_message_handler, loop, setup_consumer,
                setup_consumer_client, TimeoutMessage)


def initialize_timeout_consumer(url, name, on_message, on_final_death=None):
    @click.command()
    @click.option('--process-number', type=int, default=0,
                  help='how many of these consumers were started')
    @click.option('--message-ttl', type=int,
                  help='initial message timeout (in ms)')
    @click.option('--max-deaths', type=int,
                  help='number of tries a message can live through before '
                       'landing in the failed messages queue')
    def consumer_runner(process_number, message_ttl, max_deaths):
        consumer_tag = '{}:{}'.format(name, process_number)
        on_error = TimeoutMessage(message_ttl, max_deaths,
                                  on_final_death=on_final_death)

        on_message_args = inspect.getargspec(on_message)[0]
        if 'max_deaths' in on_message_args:
            _on_message = partial(on_message, max_deaths=max_deaths)
        else:
            _on_message = on_message
        on_message_safe = init_safe_message_handler(_on_message, on_error)
        consumer_client = setup_consumer_client(url)

        setup_consumer(consumer_client, name, consumer_tag, on_message_safe)
        loop([consumer_client])
    return consumer_runner
