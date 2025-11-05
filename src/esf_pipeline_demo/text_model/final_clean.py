# src/esf_pipeline/text_model/final_clean.py
"""Final cleaning of the text model data."""

from logging import getLogger

import pandas as pd

logger = getLogger(__name__)


def clean_training_data(
    df: pd.DataFrame,
    x: list[str],
    y: list[str],
    drop_class: list | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Final cleaning of the training data."""
    df = df.copy()
    logger.info("Final cleaning of training data...")
    df = adjust_score_industrial(df)
    logger.info("Adjusted scores based on industrial flag.")
    df = adjust_flag_ip_incompliance(df)
    logger.info("Adjusted IP incompliance based on waterproof flag.")
    df = adjust_review_scores(df)
    logger.info("Adjusted review scores based on review count.")
    df = df[~df["is_irrelevant"]].copy()
    training_cols = {
        "esf_compliant_flag": "compliant",
        "esf_non_compliant_flag": "non_compliant",
        "esf_ambiguous_flag": "ambiguous",
        "esf_irrelevant_product_flag": "irrelevant",
    }
    df = df.rename(columns=training_cols)
    source_data = df.copy()

    if isinstance(drop_class, list):
        mask = df[drop_class].any(axis=1)
        df = df[~mask].copy()
        df = df.drop(columns=drop_class)
        logger.info(f"Dropped classes {drop_class} from training data.")
    df = df[["product_id", *x, *y]]
    df[x] = df[x].fillna(0)
    df[y] = df[y].fillna(0)
    logger.info("Filled missing values with 0.")

    return df, source_data


def adjust_score_industrial(df: pd.DataFrame) -> pd.DataFrame:
    """Adjust scores based on industrial_flag and product_group rules."""

    out = df.copy()

    out.loc[out["industrial_flag"], "wattage_score"] = 0

    out.loc[
        out["industrial_flag"] & (out["product_group"] != "Group B"),
        "voltage_score",
    ] = 0

    out.loc[
        out["industrial_flag"]
        & (out["product_group"] != "Group B")
        & (out["product_group"] != "Group E"),
        "amperage_score",
    ] = 0
    score_cols = ["wattage_score", "voltage_score", "amperage_score"]
    existing_scores = [c for c in score_cols if c in out.columns]
    out[existing_scores] = out[existing_scores].fillna(0)
    out = out.drop(columns=["industrial_flag"])
    return out


def adjust_flag_ip_incompliance(df: pd.DataFrame) -> pd.DataFrame:
    """If not marketed as waterproof, treat IP incompliance as False."""
    out = df.copy()
    out["adjusted_ip_incompliance"] = out.apply(
        lambda row: row["ip_incompliance"] if row["waterproof_flag"] else False,
        axis=1,
    )
    return out


def adjust_review_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Adjust review scores based on review count."""
    out = df.copy()
    out["adjusted_negativity_score"] = (out["negativity_score"] ** 2) / (
        out["review_count"]
    )
    out["adjusted_danger_score"] = (out["danger_score"] ** 2) / (out["review_count"])
    return out
