# src/esf_pipeline/process/image_predictions.py
"""Module to process image predictions."""

import ast
from functools import partial

import pandas as pd

NON_COMPLIANCE_TAGS = [
    "Socket section/Aus-North America non-compliant",
    "Socket section/UK-German non-compliant",
]


def get_incompliance(data: list[dict] | str, minimum_probability: float = 0.5) -> dict:
    """Extract incompliance flags from image prediction data."""
    flag_map = dict.fromkeys(NON_COMPLIANCE_TAGS, 0)

    if isinstance(data, str):
        data = ast.literal_eval(data)

    for entry in data:
        if isinstance(entry, dict):
            for key, value in entry.items():
                if value >= minimum_probability:
                    for tag in NON_COMPLIANCE_TAGS:
                        if tag.lower() == key.lower():
                            flag_map[tag] = max(flag_map[tag], value)
                            break

    return flag_map


def process_image_predictions(
    df: pd.DataFrame, minimum_probability: float = 0.9
) -> pd.DataFrame:
    """Process image predictions in the DataFrame."""
    get_incompliance_ = partial(
        get_incompliance, minimum_probability=minimum_probability
    )
    df["predictions"] = df["predictions"].apply(get_incompliance_)

    out: pd.DataFrame = pd.concat(
        [df.drop(columns=["predictions"]), pd.json_normalize(df["predictions"])], axis=1
    )

    out = out.groupby("product_id").agg(
        row_count=("product_id", "size"),
        **{col: (col, "sum") for col in NON_COMPLIANCE_TAGS},
    )
    out = out.rename(columns={"row_count": "image_count"}).reset_index()

    for col in NON_COMPLIANCE_TAGS:
        out[col] = (out[col] > minimum_probability).astype(int)

    return out
