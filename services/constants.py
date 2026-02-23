from pathlib import Path

PROJECT_ROOT_PATH = Path(__file__).resolve().parent.parent
DATA_FOLDER_PATH = PROJECT_ROOT_PATH / "data"

MODEL_FOLDER_PATH = PROJECT_ROOT_PATH / "model"
ENCODER_PATH = MODEL_FOLDER_PATH / "encoder.joblib"
SCALER_PATH = MODEL_FOLDER_PATH / "scaler.joblib"
CLASSIFIER_PATH = MODEL_FOLDER_PATH / "classifier.joblib"
