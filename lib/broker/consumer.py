import logging

from confluent_kafka import Consumer, KafkaError

from lib.topics import Topics


class KafkaConsumer:
    def __init__(self, broker_url: str, group: str, topics: list[Topics]):
        self._config = {
            "bootstrap.servers": broker_url,
            "group.id": group,
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
            "security.protocol": "PLAINTEXT",
        }
        self._topics = topics
        self._consumer = None

    def __enter__(self):
        logging.debug(
            "Creating Kafka consumer with config: %s and subscribing to topics: %s",
            self._config,
            [topic.value for topic in self._topics],
        )
        self._consumer = Consumer(self._config)
        self._consumer.subscribe([topic.value for topic in self._topics])
        return self

    def __exit__(self, *unused_args, **unused_kwargs):
        self._consumer.close()
        self._consumer = None

    def consume(self, num_messages=1, timeout=1.0):
        recieved_messages = []
        for _ in range(num_messages):
            if (messages := self._consumer.poll(timeout)) is None:
                continue
            if message_error := messages.error():
                if (
                    message_error.code()
                    == KafkaError._PARTITION_EOF  # pylint: disable=protected-access
                ):
                    logging.error(message_error)
                    continue
                logging.error(message_error)
                continue
            recieved_messages.append(messages)
        return recieved_messages
