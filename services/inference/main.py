import json
import logging
from pathlib import Path

import joblib
from tap import Tap

from lib.broker.consumer import KafkaConsumer
from lib.broker.producer import KafkaProducer
from lib.pipeline import InferencePipeline
from lib.topics import Topics
from services.constants import CLASSIFIER_PATH, ENCODER_PATH, SCALER_PATH


class CommandLineArguments(Tap):
    broker_url: str = "localhost:9095"
    broker_topic_to_read: Topics = Topics.PROCESSED_DATA
    broker_topic_to_write: Topics = Topics.RESULTS
    encoder_path: Path = ENCODER_PATH
    scaler_path: Path = SCALER_PATH
    classifier_path: Path = CLASSIFIER_PATH


def load_models(encoder_path: Path, scaler_path: Path, classifier_path: Path):
    print(f"Loading models: {encoder_path}, {scaler_path}, {classifier_path}")
    return (
        joblib.load(encoder_path),
        joblib.load(scaler_path),
        joblib.load(classifier_path),
    )


def process_messages(
    messages, inference_pipeline: InferencePipeline, producer: KafkaProducer
):
    for message in messages:
        data = json.loads(message.value().decode("utf-8"))
        try:
            result = inference_pipeline.infer(data)
        except Exception as e:
            logging.error("Error processing message: %s", e)
            continue
        producer.send(key="1", value=json.dumps(result))
        logging.debug("Sent result: %s", result)


def main(args: CommandLineArguments) -> None:
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Starting pipeline...")

    encoder, scaler, classifier = load_models(
        args.encoder_path, args.scaler_path, args.classifier_path
    )

    consumer = KafkaConsumer(
        broker_url=args.broker_url, group="model", topics=[args.broker_topic_to_read]
    )
    producer = KafkaProducer(
        broker_url=args.broker_url, topic=args.broker_topic_to_write
    )
    inference_pipeline = InferencePipeline(encoder, scaler, classifier)
    with consumer, producer:
        while True:
            messages = consumer.consume(num_messages=1, timeout=10)
            if not messages:
                logging.warning("Received no messages")
                continue
            process_messages(messages, inference_pipeline, producer)


if __name__ == "__main__":
    ARGUMENTS = CommandLineArguments(underscores_to_dashes=True).parse_args()
    main(ARGUMENTS)
