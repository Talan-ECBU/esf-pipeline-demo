# scripts/text_model.py
"""Script to create the text model for ESF data."""

import argparse
from pathlib import Path

import pandas as pd

from esf_pipeline.config import (
    LOCAL_MODEL_DIR,
    LOCAL_PROCESSED_DIR,
    LOCAL_RAW_DIR,
    setup_logging,
)
from esf_pipeline.process.cleaning import clean_product, clean_reviews
from esf_pipeline.process.standardise import (
    standardise_product_data,
    standardise_reviews,
)
from esf_pipeline.text_model.final_clean import clean_training_data
from esf_pipeline.text_model.multi_class_model import train_and_evaluate

setup_logging()

Y = ["compliant", "non_compliant"]

X = [
    "voltage_score",
    "amperage_score",
    "wattage_score",
    "is_recall_brand",
    "adjusted_danger_score",
]


def clean():
    root_dir = LOCAL_RAW_DIR / "esf_test_data"
    product_data = clean_product(
        standardise_product_data(root_directory=root_dir, is_model=True),
        save=False,
    )
    product_data = product_data.to_pandas()
    spot_check_data = pd.read_csv(LOCAL_RAW_DIR / "esf_spotcheck_results.csv")
    output_data = product_data.merge(
        spot_check_data,
        on=["product_id", "product_group"],
        how="right",
    )
    review_data, _ = clean_reviews(
        standardise_reviews(root_directory=root_dir), save=False
    )
    review_data = review_data.to_pandas()
    review_data = review_data[
        [
            "product_id",
            "review_text",
            "negativity_score",
            "danger_score",
        ]
    ]
    scoring_data = (
        review_data.groupby("product_id")
        .agg({"negativity_score": "sum", "danger_score": "sum", "review_text": "count"})
        .rename(columns={"review_text": "review_count"})
        .reset_index()
    )
    output_data = output_data.merge(
        scoring_data,
        on=["product_id"],
        how="left",
    )
    output_data.to_csv(LOCAL_PROCESSED_DIR / "esf_spotcheck_results.csv", index=False)


def train():
    training_data_filepath = (
        Path(__file__).parents[1] / "data" / "processed" / "esf_spotcheck_results.csv"
    )
    training_data = pd.read_csv(training_data_filepath)
    training_data, source_data = clean_training_data(
        training_data, X, Y, drop_class=["ambiguous", "irrelevant"]
    )

    model, metrics, misclassified = train_and_evaluate(
        training_data=training_data,
        x_columns=X,
        y_columns=Y,
        plot_confusion=True,
        intercept=True,
    )

    coef = model.coef_
    positive_class = model.classes_[1] if coef.shape[0] == 1 else model.classes_
    idx = [positive_class] if isinstance(positive_class, str) else list(positive_class)
    coefficients = pd.DataFrame(coef, columns=X, index=idx)

    if hasattr(model, "intercept_"):
        coefficients["intercept"] = model.intercept_

    source_data.to_csv(LOCAL_MODEL_DIR / "source_data.csv", index=False)
    coefficients.to_csv(LOCAL_MODEL_DIR / "model_coef.csv")
    misclassified.to_csv(LOCAL_MODEL_DIR / "misclassified_data.csv", index=False)
    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv(LOCAL_MODEL_DIR / "model_metrics.csv", index=False)


def main():
    parser = argparse.ArgumentParser(description="Build and train the text model.")
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean and prepare the training data.",
    )
    parser.add_argument(
        "--train",
        action="store_true",
        help="Train the text model.",
    )
    parser.add_argument(
        "--build",
        action="store_true",
        help="Run both cleaning and training.",
    )
    args = parser.parse_args()

    if args.clean:
        clean()
    if args.train:
        train()
    if args.build:
        clean()
        train()


if __name__ == "__main__":
    main()
