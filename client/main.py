import json
import logging

import pandas as pd
import streamlit as st
from tap import Tap

from lib.broker.consumer import KafkaConsumer
from lib.topics import Topics


class CommandLineArguments(Tap):
    broker_url: str = "localhost:9095"
    broker_topic_to_read: Topics = Topics.RESULTS


class VisualizationService:
    def __init__(self):
        st.set_page_config(page_title="Real-Time Data Dashboard")
        for name in [
            "metrics",
            "predicted",
            "target",
            "encoder_time",
            "scaler_time",
            "classifier_time",
            "total_time",
        ]:
            self.setup_session_state(name)

        self._chart_holder_title = st.empty()
        self._chart_holder_metrics = st.empty()
        self._predicted_vs_real_title = st.empty()
        self._predicted_vs_real_chart = st.empty()
        self._performance_title = st.empty()
        self._performance_chart = st.empty()

    @staticmethod
    def setup_session_state(name: str):
        if name not in st.session_state:
            st.session_state[name] = []

    def render_message(self, messages):
        for message in messages:
            if message.error():
                print(f"Error in message: {message.error()}")
                continue

            results_data = json.loads(message.value().decode("utf-8"))

            st.session_state["metrics"].append(results_data["metrics"])
            st.session_state["predicted"].append(*results_data["rating"])
            st.session_state["target"].append(*results_data["original"]["target"])

            st.session_state["encoder_time"].append(
                results_data["performance"]["encoder_time"] * 1000
            )
            st.session_state["scaler_time"].append(
                results_data["performance"]["scaler_time"] * 1000
            )
            st.session_state["classifier_time"].append(
                results_data["performance"]["classifier_time"] * 1000
            )
            st.session_state["total_time"].append(
                sum(
                    [
                        results_data["performance"]["encoder_time"],
                        results_data["performance"]["scaler_time"],
                        results_data["performance"]["classifier_time"],
                    ]
                )
                * 1000
            )

        df_f1 = pd.DataFrame.from_dict(st.session_state["metrics"])

        self._chart_holder_title.title("F1 over time")
        self._chart_holder_metrics.line_chart(df_f1)

        df_predictions = pd.DataFrame(
            {
                "Predicted rating": st.session_state["predicted"],
                "Real rating": st.session_state["target"],
            }
        )

        self._predicted_vs_real_title.title("Predicted vs Real Rating")
        self._predicted_vs_real_chart.line_chart(df_predictions)

        df_performance = pd.DataFrame(
            {
                "Encoder time, ms": st.session_state["encoder_time"],
                "Scaler time, ms": st.session_state["scaler_time"],
                "Classifier time, ms": st.session_state["classifier_time"],
                "Total time, ms": st.session_state["total_time"],
            }
        )

        self._performance_title.title("Performance Metrics")
        self._performance_chart.line_chart(df_performance)


def main(args: CommandLineArguments) -> None:
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Starting visualization service...")

    visualization_service = VisualizationService()

    consumer = KafkaConsumer(
        broker_url=args.broker_url,
        group="dashboard",
        topics=[args.broker_topic_to_read],
    )

    with consumer:
        while True:
            messages = consumer.consume(num_messages=1, timeout=5)
            if not messages:
                logging.warning("Received no messages")
                continue
            visualization_service.render_message(messages)


if __name__ == "__main__":
    ARGUMENTS = CommandLineArguments(underscores_to_dashes=True).parse_args()
    main(ARGUMENTS)
