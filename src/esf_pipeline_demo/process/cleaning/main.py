# src/esf_pipeline/process/cleaning/product_text.py
"""
Module containing main functionalities to clean text-based data collected from
scraping the online marketplaces.
"""

import json
import re
from logging import getLogger

import pandas as pd
import polars as pl

from ...config.config import DATA_SCHEMA, LOCAL_PROCESSED_DIR, LOCAL_RAW_DIR, RECALL_REF
from ..common import standardise_text_encoding
from .score_product import provide_compliance_scores
from .score_review import provide_feedback_scores

logger = getLogger(__name__)


def clean_product(product_data: pd.DataFrame, save: bool = True) -> pl.DataFrame:
    """Clean text columns in the product dataframe."""
    df = provide_compliance_scores(product_data)
    for col in ["voltage_info", "amperage_info", "wattage_info"]:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )
    df = pl.from_pandas(df)
    df = _get_feature_flags(df)
    df = _flag_irrelevant_products(df)
    df = _flag_recall(df)
    df = _get_ip_rating(df)

    for col in df.columns:
        dtype = df.schema[col]
        if isinstance(dtype, (pl.List, pl.Struct, pl.Array)):
            df = df.with_columns(
                pl.col(col).map_elements(
                    lambda x: (
                        json.dumps(x, ensure_ascii=False) if x is not None else None
                    ),
                    return_dtype=pl.Utf8,
                )
            )

    max_str_length = 10_000
    df = df.with_columns(
        pl.when(pl.col("text").str.len_chars() > max_str_length)
        .then(pl.col("text").str.slice(0, max_str_length))
        .otherwise(pl.col("text"))
        .alias("text")
    )

    standardise_text_encoding(df)
    if save:
        df.write_csv(
            LOCAL_PROCESSED_DIR / "product_data.csv",
            include_header=True,
        )
    return df


def clean_reviews(review_data: pd.DataFrame, save: bool = True) -> pl.DataFrame:
    """Clean and score review data, saving detailed and aggregated outputs."""
    review_data = pl.from_pandas(review_data)

    review_data = provide_feedback_scores(review_data, feedback_col="content")

    review_data = review_data.select(
        [
            "product_id",
            "marketplace",
            "review_text",
            "negativity_score",
            "danger_score",
        ]
    )
    review_data = standardise_text_encoding(review_data)
    if save:
        review_data.write_csv(LOCAL_PROCESSED_DIR / "review_data.csv")

    scoring_data = review_data.group_by(["product_id", "marketplace"]).agg(
        [
            pl.col("negativity_score").sum().alias("negativity_score"),
            pl.col("danger_score").sum().alias("danger_score"),
            pl.count("review_text").alias("review_count"),
        ]
    )

    if save:
        scoring_data.write_csv(LOCAL_PROCESSED_DIR / "review_scores.csv")
    return review_data, scoring_data


def _get_feature_flags(df: pl.DataFrame) -> pl.DataFrame:
    """Add adapter, industrial and waterproof flags to the dataframe."""
    logger.info("Getting Adapter, Industrial and Waterproof flags...")

    waterproof_regex = r"""(?ix)
        \bwater\s*proof(?:ed)?\b |
        \bwater\s*resistant\b |
        \bsplash\s*proof\b |
        \bsplash\s*resistant\b |
        \bsealed\s*(?:against)?\s*water\b |
        \bmoisture\s*(?:sealed|resistant)\b |
        \bweather\s*(?:proof|resistant)\b
    """

    return df.with_columns(
        [
            pl.col("text")
            .str.contains("adapter", literal=False)
            .fill_null(False)
            .alias("adapter_flag"),
            pl.col("text")
            .str.contains("industrial|commercial", literal=False)
            .fill_null(False)
            .alias("industrial_flag"),
            pl.col("text")
            .str.contains(waterproof_regex, literal=False)
            .fill_null(False)
            .alias("waterproof_flag"),
        ]
    )


def _flag_irrelevant_products(df: pl.DataFrame) -> pl.DataFrame:
    """Flag products as irrelevant based on keywords and schema."""
    logger.info("Flagging irrelevant products...")
    dfs = []

    for group, group_data in DATA_SCHEMA.items():
        subset = df.filter(pl.col("product_group") == group)

        irrelevant_mask = (
            pl.col("text")
            .str.contains("filter|bag|backpacks|case", literal=False)
            .fill_null(False)
            & pl.col("voltage_info").is_null()
            & pl.col("amperage_info").is_null()
            & pl.col("wattage_info").is_null()
        )

        irrelevant_cat = group_data.get("irrelevant_categories", [])
        high_volts_only = group_data.get("high_volts_only", False)

        if high_volts_only:
            usb_mask = (
                pl.col("text").str.contains("usb", literal=False).fill_null(False)
            )
            irrelevant_mask = irrelevant_mask | usb_mask

        subset = subset.with_columns(
            pl.when(pl.col("category").is_in(irrelevant_cat) | irrelevant_mask)
            .then(True)
            .otherwise(False)
            .alias("is_irrelevant")
        )
        dfs.append(subset)

    df = pl.concat(dfs)

    irrelevant_products = pl.read_csv(LOCAL_RAW_DIR / "irrelevant_products.csv")
    irrelevant_pairs = set(
        zip(
            irrelevant_products["product_id"],
            irrelevant_products["product_group"],
            strict=True,
        )
    )

    df = df.with_columns(
        pl.struct(["product_id", "product_group"])
        .map_elements(
            lambda s: (s["product_id"], s["product_group"]) in irrelevant_pairs
        )
        .alias("irrelevant_pair_flag")
    )

    df = df.with_columns(
        (pl.col("is_irrelevant") | pl.col("irrelevant_pair_flag")).alias(
            "is_irrelevant"
        )
    ).drop("irrelevant_pair_flag")

    num_irrelevant = df["is_irrelevant"].sum()
    logger.info(f"Flagged {num_irrelevant} products as irrelevant")

    return df


def _flag_recall(df: pl.DataFrame) -> pl.DataFrame:
    """Flag products as recalled based on known models and brands."""
    logger.info("Flagging recalled products...")

    recall_models_regex = "|".join(re.escape(m) for m in RECALL_REF["recall_models"])
    recall_brands_regex = "|".join(re.escape(b) for b in RECALL_REF["recall_brands"])

    df = df.with_columns(
        [
            pl.col("text")
            .str.contains(recall_models_regex, literal=False)
            .fill_null(False)
            .alias("recalled_flag"),
        ]
    )

    df = df.with_columns(
        [
            (
                pl.col("recalled_flag")
                | pl.col("product_id").is_in(RECALL_REF["recall_ids"])
            ).alias("recalled_flag")
        ]
    )

    df = df.with_columns(
        [
            pl.col("manufacturer")
            .fill_null("")
            .str.strip_chars()
            .alias("manufacturer"),
        ]
    )

    df = df.with_columns(
        [
            (
                pl.col("manufacturer")
                .str.to_lowercase()
                .is_in([b.lower() for b in RECALL_REF["recall_brands"]])
                | pl.col("text")
                .str.contains(recall_brands_regex, literal=False)
                .fill_null(False)
            ).alias("is_recall_brand")
        ]
    )

    return df


def _get_ip_rating(df: pl.DataFrame) -> pl.DataFrame:
    """Extract IP rating and split into ingress/moisture digits."""
    logger.info("Extracting IP ratings from text...")

    ip_pattern = r"(?i)\bIP([0-9X])([0-9])\b"

    df = df.with_columns(
        [
            pl.col("text").str.extract(ip_pattern, group_index=0).alias("ip_rating"),
            pl.col("text").str.extract(ip_pattern, group_index=1).alias("ip_ingress"),
            pl.col("text").str.extract(ip_pattern, group_index=2).alias("ip_moisture"),
        ]
    )

    df = df.with_columns(
        [
            pl.col("ip_ingress")
            .str.to_uppercase()
            .replace("X", None)
            .cast(pl.Int64)
            .alias("ip_ingress"),
            pl.col("ip_moisture")
            .str.to_uppercase()
            .replace("X", None)
            .cast(pl.Int64)
            .alias("ip_moisture"),
        ]
    )

    rating_threshold = 4
    df = df.with_columns(
        [(pl.col("ip_moisture") < rating_threshold).alias("ip_incompliance")]
    )

    return df
