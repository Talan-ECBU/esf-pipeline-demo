# scripts/run_final_predictions.py
"""Script to run the final prediction model and save results."""

import argparse

import numpy as np
import pandas as pd

from esf_pipeline.config import LOCAL_MODEL_DIR, LOCAL_PROCESSED_DIR, setup_logging
from esf_pipeline.process.image_predictions import (
    NON_COMPLIANCE_TAGS,
    process_image_predictions,
)
from esf_pipeline.text_model.final_clean import (
    adjust_flag_ip_incompliance,
    adjust_review_scores,
    adjust_score_industrial,
)

setup_logging()


def get_models():
    """
    Clean and merge product, review, and image model (result data) for
    final prediction.
    """
    image_data_filepath = LOCAL_PROCESSED_DIR / "image_predictions.csv"
    if not image_data_filepath.exists():
        raise FileNotFoundError(
            "Please run image prediction (custom vision) script first."
        )
    image_data = pd.read_csv(image_data_filepath)

    processed_predictions = process_image_predictions(image_data)
    processed_predictions.to_csv(
        LOCAL_MODEL_DIR / "processed_image_predictions.csv", index=False
    )

    product_data_filepath = LOCAL_PROCESSED_DIR / "product_data.csv"
    if not product_data_filepath.exists():
        raise FileNotFoundError("Please run product data processing script first.")
    product_data = pd.read_csv(product_data_filepath)

    merged_data = product_data.merge(processed_predictions, on="product_id", how="left")

    review_data_filepath = LOCAL_PROCESSED_DIR / "review_scores.csv"
    if not review_data_filepath.exists():
        raise FileNotFoundError("Please run review data processing script first.")
    review_data = pd.read_csv(review_data_filepath)

    merged_data = merged_data.merge(
        review_data, on=["product_id", "marketplace"], how="left"
    )

    merged_data = adjust_score_industrial(merged_data)
    merged_data = adjust_flag_ip_incompliance(merged_data)
    merged_data = adjust_review_scores(merged_data)
    max_text_len = 10_000
    merged_data["text"] = merged_data["text"].astype(str).str.slice(0, max_text_len)
    merged_data.to_csv(LOCAL_MODEL_DIR / "all_prediction_data.csv", index=False)


def load_coefficients(csv_path: str) -> tuple[dict, float]:
    """Load coefficients and intercept from a one-row CSV."""
    df = pd.read_csv(csv_path, index_col=0)
    row = df.iloc[0].to_dict()

    intercept = float(row.pop("intercept"))
    coefs = {k: float(v) for k, v in row.items()}
    return coefs, intercept


def apply_logistic(
    df: pd.DataFrame, coefs: dict, intercept: float, output_col: str = "predicted_prob"
) -> pd.DataFrame:
    """
    Apply logistic regression to a DataFrame.

    Assumes DataFrame has the same columns as the coefficients dict. Adds a
    probability column.
    """
    logit = intercept
    for feature, coef in coefs.items():
        if feature in df.columns:
            logit += df[feature] * coef
        else:
            raise KeyError(f"Column '{feature}' missing from DataFrame")

    df = df.copy()
    df[output_col] = 1 / (1 + np.exp(-logit))
    return df


def predict():
    data_path = LOCAL_MODEL_DIR / "all_prediction_data.csv"
    coef_path = LOCAL_MODEL_DIR / "model_coef.csv"
    output_path = LOCAL_MODEL_DIR / "final_predictions.csv"

    data = pd.read_csv(data_path)
    coefs, intercept = load_coefficients(coef_path)

    predictions: pd.DataFrame = apply_logistic(
        df=data, coefs=coefs, intercept=intercept, output_col="text_nc_prob"
    )

    predictions["adjusted_ip_incompliance"] = predictions[
        "adjusted_ip_incompliance"
    ].fillna(False)

    # Image non-compliance
    for col in NON_COMPLIANCE_TAGS:
        predictions[col] = predictions[col].fillna(False)
        predictions[col] = predictions[col].astype(bool)

    text_pred_threshold = 0.5
    predictions["final_non_compliant"] = (
        (predictions["text_nc_prob"] > text_pred_threshold)
        | predictions["adjusted_ip_incompliance"]
        | predictions["Socket section/Aus-North America non-compliant"]
        | predictions["Socket section/UK-German non-compliant"]
        | predictions["recalled_flag"].astype(bool)
    )
    predictions["final_non_compliant"] = predictions["final_non_compliant"].astype(bool)

    predictions.to_csv(output_path, index=False)


def main():
    parser = argparse.ArgumentParser(description="Run final prediction model.")
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Final stage cleaning and merging of product, review and image models.",
    )
    parser.add_argument(
        "--predict",
        action="store_true",
        help="Run the prediction model.",
    )
    args = parser.parse_args()

    if args.clean:
        get_models()
    if args.predict:
        predict()
    if not (args.clean or args.predict):
        get_models()
        predict()


if __name__ == "__main__":
    main()
