import json
import logging

import pandas as pd
from tap import Tap

from lib.broker.consumer import KafkaConsumer
from lib.broker.producer import KafkaProducer
from lib.topics import Topics


class CommandLineArguments(Tap):
    broker_url: str = "localhost:9097"
    broker_topic_to_read: Topics = Topics.RAW_DATA
    broker_topic_to_write: Topics = Topics.PROCESSED_DATA


def remove_nan_and_duplicates(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset.dropna(inplace=True)
    dataset.drop_duplicates(inplace=True)
    return dataset


def remove_unnecessary_columns(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset.drop(
        columns=[
            "title",
            "imdb_id",
            "original_language",
            "original_title",
            "overview",
            "tagline",
            "genres",
            "production_companies",
            "production_countries",
            "spoken_languages",
            "keywords",
        ],
        inplace=True,
    )
    return dataset


def convert_features(dataset: pd.DataFrame) -> pd.DataFrame:
    dataset["adult"] = dataset["adult"].astype(int)
    dataset["release_date"] = pd.to_datetime(dataset["release_date"], errors="coerce")
    dataset.dropna(inplace=True)
    dataset["release_year"] = dataset["release_date"].dt.year
    dataset.drop(columns="release_date", inplace=True)
    return dataset


def process_message(message):
    if message.error():
        raise ValueError(f"Error in message: {message.error()}")

    dataset = json.loads(message.value().decode("utf-8"))
    logging.info("Preprocessing received dataset: %s", dataset)

    df = pd.DataFrame([dataset])

    processed_data = (
        df.pipe(remove_unnecessary_columns)
        .pipe(remove_nan_and_duplicates)
        .pipe(convert_features)
    )

    target = processed_data["vote_average"].apply(int).to_numpy().tolist()
    features = processed_data.drop(columns=["vote_average"]).to_dict(orient="records")

    if not features or not target:
        raise ValueError("Empty features or target after preprocessing!")

    return {"features": features, "target": target}


def main(arguments: CommandLineArguments):
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Starting preprocessing...")

    with (
        KafkaConsumer(
            broker_url=arguments.broker_url,
            group="preprocessor",
            topics=[arguments.broker_topic_to_read],
        ) as consumer,
        KafkaProducer(
            broker_url=arguments.broker_url, topic=arguments.broker_topic_to_write
        ) as producer,
    ):
        while True:
            if not (messages := consumer.consume(num_messages=1, timeout=10)):
                logging.warning(
                    "No messages received in preprocessing from topic %s",
                    arguments.broker_topic_to_read.value,
                )
                continue
            for message in messages:
                try:
                    processed_message = process_message(message)
                except Exception as e:
                    logging.warning("Error during preprocessing: %s", e)
                    continue

                logging.info("Sending processed data: %s", processed_message)
                producer.send(key="1", value=json.dumps(processed_message))


if __name__ == "__main__":
    ARGUMENTS = CommandLineArguments(underscores_to_dashes=True).parse_args()
    main(ARGUMENTS)
