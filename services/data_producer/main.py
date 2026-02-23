import json
import logging
import random
import time
from pathlib import Path

import kagglehub
import pandas as pd
from tap import Tap

from lib.broker.producer import KafkaProducer
from lib.topics import Topics
from services.constants import DATA_FOLDER_PATH


class CommandLineArguments(Tap):
    dataset_path: Path = DATA_FOLDER_PATH / "test_data.csv"
    broker_url: str = "localhost:9097"
    broker_topic: Topics = Topics.RAW_DATA


def send_raw_data(
    dataset: pd.DataFrame,
    producer: KafkaProducer,
    default_sleep_duration: float | None = None,
):
    for _, record in dataset.iterrows():
        record_dict = record.to_dict()

        producer.send(key="1", value=json.dumps(record_dict))

        time_to_sleep = default_sleep_duration or random.uniform(1, 5)
        time.sleep(time_to_sleep)


def main(arguments: CommandLineArguments) -> None:
    logging.basicConfig(level=logging.DEBUG)

    try:
        dataset = pd.read_csv(arguments.dataset_path)
    except Exception:
        logging.warning("Something wrong with local dataset. Downloading...")
        original_datatest_path = kagglehub.dataset_download("anandshaw2001/imdb-data")
        dataset_path = f"{original_datatest_path}/Imdb Movie Dataset.csv"
        dataset = pd.read_csv(dataset_path)
        dataset.to_csv(arguments.dataset_path, index=False)

    with KafkaProducer(
        broker_url=arguments.broker_url, topic=arguments.broker_topic
    ) as producer:
        send_raw_data(dataset, producer, 3)


if __name__ == "__main__":
    ARGUMENTS = CommandLineArguments(underscores_to_dashes=True).parse_args()
    main(ARGUMENTS)
