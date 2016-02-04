consumerlib
===========

This is a simple library for building AMQP consumers.
It allows you to easily create runnable scripts that process incoming messages for an AMQP queue.
It also allows for messages that produce uncaught exceptions to be put on timeout and be automatically reinserted into the queue.
After a specified amount of "deaths" you can again custom-handle it.
Messages will be processed asynchroniously.

# Install

```shell
pip install -U https://github.com/riyad/python-consumerlib.git
```

# Run Tests

```shell
pip install -Ur test-requirements.txt
py.test tests/
```

# Examples

There is an example consumer in `examples/consumer.py`.
It will run and wait for messages from a queue defined by the constants at the top.

```shell
python -m "examples.consumer" setup
python -m "examples.consumer" run --message-ttl=1000 --max-deaths=2
```

# Contact and Issues

Please, report all issues on our issue tracker on GitHub: https://gitlab.com/riyad/python-consumerlib
