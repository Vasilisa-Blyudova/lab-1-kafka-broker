import logging
import time

import pandas as pd
from sklearn import metrics


class InferencePipeline:
    def __init__(self, encoder, scaler, classifier):
        self._encoder = encoder
        self._scaler = scaler
        self._classifier = classifier
        self._columns_to_drop = [
            "ohe__status_In Production",
            "ohe__status_Planned",
            "ohe__status_Rumored",
            "remainder__runtime",
            "remainder__adult",
            "remainder__release_year",
        ]
        self._comulative_y_true = []
        self._comulative_y_pred = []

    def encode(self, features):
        transformed_features = self._encoder.transform(features)
        df_features = pd.DataFrame(
            transformed_features, columns=self._encoder.get_feature_names_out()
        )
        df_features.drop(columns=self._columns_to_drop, inplace=True, errors="ignore")
        return df_features

    def calculate_f1(self, data, predictions):
        target = data.get("target")
        if target is None:
            return None
        if not isinstance(target, list):
            target = [target]
        if len(target) != len(predictions):
            logging.warning(
                "Mismatch in lengths of true labels %d and predictions %d.",
                len(target),
                len(predictions),
            )
            return None
        self._comulative_y_true.extend(target)
        self._comulative_y_pred.extend(predictions)
        return metrics.f1_score(
            self._comulative_y_true, self._comulative_y_pred, average="weighted"
        )

    def infer(self, data):
        logging.debug("Pipeline received data: %s", data)

        features = pd.json_normalize(data["features"])
        start_encode_time = time.time()
        transformed_features = self.encode(features)
        finish_encode_time = time.time()
        scaled_features = self._scaler.transform(transformed_features)
        finish_scaler_time = time.time()
        predictions = self._classifier.predict(scaled_features).tolist()
        finish_classifier_time = time.time()
        return {
            "original": data,
            "rating": predictions,
            "metrics": {"F1": self.calculate_f1(data, predictions)},
            "performance": {
                "encoder_time": finish_encode_time - start_encode_time,
                "scaler_time": finish_scaler_time - finish_encode_time,
                "classifier_time": finish_classifier_time - finish_scaler_time,
            },
        }
