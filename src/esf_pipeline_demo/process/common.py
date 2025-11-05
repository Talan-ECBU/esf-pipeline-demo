# src/esf_pipeline/process/common.py
"""Common utilities for processing marketplace data."""

import re
import unicodedata

import pandas as pd
import polars as pl

# Keywords for search functions
VOLTAGE_STEM = "volt"
VOLTAGE_KEYWORDS = ["voltage"]
WATTAGE_STEM = "watt"
WATTAGE_KEYWORDS = ["maximum_power", "horsepower", "kw", "mw", "hp"]
AMPERAGE_KEYWORDS = ["amperage", "ampere", "amp"]


def normalise_and_join_text_cols(
    df: pd.DataFrame,
    *text_cols: str,
    sep: str = "||",
    ignore_missing: bool = True,
    normalise_fn=None,
) -> pd.Series:
    """Normalize and concatenate text columns into a single string."""
    if not text_cols:
        return pd.Series("", index=df.index)

    if normalise_fn is None:
        normalise_fn = normalise_text

    parts: list[pd.Series] = []
    for col in text_cols:
        if col not in df.columns:
            if not ignore_missing:
                raise KeyError(f"Column '{col}' not found in DataFrame.")
            parts.append(pd.Series("", index=df.index))
        else:
            s = df[col].fillna("").astype(str).map(normalise_fn)
            # ensure dtype is string for str.cat
            parts.append(s.astype("string"))

    head, tail = parts[0], parts[1:]
    return head.str.cat(tail, sep=sep)


def normalise_text(s: str | dict | list) -> str:
    """
    Normalize a string by removing zero-width characters and standardizing
    dashes.
    """
    if isinstance(s, dict) or isinstance(s, list):
        s = repr(s)
    if not isinstance(s, str):
        s = "" if pd.isna(s) else str(s)
    s = s.translate(
        {
            0x200B: None,
            0x200C: None,
            0x200D: None,
            0x200E: None,
            0x200F: None,
            0xFEFF: None,
        }
    )
    s = s.replace("\u2013", "-").replace("\u2014", "-").replace("\u2212", "-")
    return s


def standardise_text_encoding(df: pl.DataFrame) -> pl.DataFrame:
    """
    Clean all text columns in a Polars DataFrame by removing line separators,
    paragraph separators, and non-UTF-8/unusual Unicode characters.

    Parameters
    ----------
    df : pl.DataFrame
        The DataFrame containing the text columns.

    Returns
    -------
    pl.DataFrame
        The DataFrame with cleaned text columns.
    """

    text_cols = [
        col
        for col, dtype in zip(df.columns, df.dtypes, strict=False)
        if dtype == pl.Utf8
    ]

    # Apply the cleaning logic lazily across all Utf8 columns
    for col in text_cols:
        df = df.with_columns(
            pl.col(col).map_elements(_clean_string, return_dtype=pl.Utf8).alias(col)
        )

    return df


def _clean_string(s: str) -> str:
    """Helper function for standardise_text_encoding"""
    if not isinstance(s, str):
        return s

    # Normalize Unicode (fix weird diacritics)
    s = unicodedata.normalize("NFKC", s)

    # Remove line and paragraph separators (U+2028, U+2029)
    s = s.replace("\u2028", " ").replace("\u2029", " ")

    # Remove other control characters and non-printables
    s = re.sub(r"[\x00-\x1F\x7F-\x9F]+", " ", s)

    # Collapse multiple spaces and strip
    s = re.sub(r"\s+", " ", s).strip()

    return s
