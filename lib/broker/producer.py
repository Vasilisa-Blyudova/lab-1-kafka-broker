import logging

from confluent_kafka import Producer

from lib.topics import Topics

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, broker_url: str, topic: Topics):
        self._producer_config = {
            "bootstrap.servers": broker_url,
            "acks": "all",  # Wait confirmation from ISR
            "retries": 5,
            "linger.ms": 10,
            "security.protocol": "PLAINTEXT",
        }
        self._producer = None
        self._topic = topic

    def __enter__(self):
        self._producer = Producer(self._producer_config)
        return self

    def __exit__(self, *unused_args, **unused_kwargs):
        self._producer.flush()
        self._producer = None

    def send(self, key, value):
        try:
            logger.info(
                "Sending message to topic %s with key %s: %s",
                self._topic.value,
                key,
                value,
            )
            self._producer.produce(
                self._topic.value, key=key, value=value, callback=self._report_status
            )
            self._producer.poll(1)
            self._producer.flush()
        except Exception as e:
            logger.error("Failed to send message: %s", e)
            raise e

    def _report_status(self, err: str, message: str) -> None:
        if err is not None:
            logger.error("Error sending the message: %s", err)
            return
        logger.info("Successfully delivered the message: %s", message)
