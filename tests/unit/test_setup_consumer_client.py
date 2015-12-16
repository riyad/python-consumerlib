# -*- coding: utf-8 -*-
from mock import patch
import puka
from consumerlib import setup_consumer_client


def test_returns_client(amqp_url):
    client = setup_consumer_client(amqp_url)
    assert isinstance(client, puka.Client)


@patch("consumerlib.puka")
def test_client_uses_defautl_amqp(puka, amqp_url):
    setup_consumer_client(amqp_url)
    puka.Client.assert_called_once_with(amqp_url)


@patch("consumerlib.puka")
def test_client_is_connected(puka, amqp_url):
    client = setup_consumer_client(amqp_url)
    client.wait.assert_called_once_with(client.connect())
