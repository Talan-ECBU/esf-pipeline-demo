# src/esf_pipeline/process/standardise/main.py
"""Main entrypoint to the ESF data standardisation application."""

import importlib
import json
import logging
import os
import pkgutil
import re
from collections.abc import Callable

import pandas as pd

from ...config import LOCAL_PROCESSED_DIR
from . import marketplaces

logger = logging.getLogger(__name__)

# Standardised product columns
PRODUCT_COLS = [
    "product_id",
    "marketplace",
    "product_group",
    "title",
    "text",
    "url",
    "seller_id",
    "amperage_info",
    "voltage_info",
    "wattage_info",
    "query",
    "category",
    "manufacturer",
]


def standardise_product_data(
    root_directory: str, is_model: bool = False
) -> pd.DataFrame:
    """Standardise product data from multiple marketplaces."""
    package = marketplaces
    final_product_data = []
    for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
        if is_pkg:
            continue  # skip subpackages if any
        full_module_name = f"{package.__name__}.{module_name}"
        module = importlib.import_module(full_module_name)
        process_func: Callable = getattr(module, "process", None)
        raw_data = _collect_data(
            root_directory=root_directory,
            marketplace=module_name,
            target="product",
        )
        if not raw_data:
            logger.warning(f"No data to process for {module_name}. Skipping...")
            continue

        try:
            standardised_products, standardised_sellers = process_func(raw_data)
            standardised_products["marketplace"] = module_name
            standardised_products["text"] = standardised_products["text"].apply(
                _clean_text
            )
            if not is_model:
                if "category" not in standardised_products.columns:
                    standardised_products["category"] = standardised_products["query"]
                os.makedirs(
                    LOCAL_PROCESSED_DIR / "individual_marketplace",
                    exist_ok=True,
                )
                standardised_products.to_csv(
                    LOCAL_PROCESSED_DIR
                    / f"individual_marketplace/{module_name}_product_data.csv",
                    index=False,
                )
                standardised_sellers.to_csv(
                    LOCAL_PROCESSED_DIR
                    / f"individual_marketplace/{module_name}_seller_data.csv",
                    index=False,
                )
            final_product_data.append(standardised_products[PRODUCT_COLS].copy())

        except Exception as e:
            logger.exception(f"Error processing data for {module_name}: {e}")
            continue

    if final_product_data:
        all_products = pd.concat(final_product_data, ignore_index=True)
    else:
        logger.warning("No product data was standardised from any marketplace.")
        raise ValueError("No product data to return.")
    return all_products


def standardise_reviews(
    root_directory: str,
) -> pd.DataFrame:
    """Standardise review data from multiple marketplaces."""
    package = marketplaces
    # Get legacy ID fields for backward compatibility
    legacy_id_fields = []
    for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
        if is_pkg:
            continue  # skip subpackages if any
        full_module_name = f"{package.__name__}.{module_name}"
        module = importlib.import_module(full_module_name)
        legacy_id_field = getattr(module, "LEGACY_ID_FIELD", None)
        if legacy_id_field:
            legacy_id_fields.append(legacy_id_field)

    standardised_reviews = _collect_data(
        root_directory=root_directory,
        target="review",
        legacy_id_fields=legacy_id_fields,
    )
    standardised_reviews = pd.DataFrame(standardised_reviews)
    return standardised_reviews


def _collect_data(
    root_directory: str,
    marketplace: str | None = None,
    target: str = "product",
    legacy_id_fields: list | None = None,
) -> list[dict]:
    """
    Recursively searches for JSON files in the directory for a single
    marketplace; apply the relevant marketplace specific standardisation and
    combines them into a single list of dictionaries.
    """
    raw_data = []
    group_text_pattern = re.compile(r"(Group\s+[A-Z])", re.IGNORECASE)
    for root, _, files in os.walk(root_directory):
        if marketplace and marketplace not in root:
            continue
        logger.debug(f"Processing {marketplace or 'ALL'} folder: {root}")
        for file in files:
            if file.endswith(".json") and target in file.lower():
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list) and target == "product":
                            for item in data:
                                if isinstance(item, dict):
                                    item["product_group"] = group_text_pattern.search(
                                        root
                                    ).group()
                                raw_data.append(item)
                        elif isinstance(data, list) and target == "review":
                            for item in data:
                                entries = item.get("reviews", [])
                                product_id = item.get("product_id") or next(
                                    (
                                        item.get(f)
                                        for f in legacy_id_fields
                                        if item.get(f) is not None
                                    ),
                                    None,
                                )
                                market_matched = re.search(
                                    r"marketplace=([^\\/]+)", root
                                )
                                marketplace_string = (
                                    market_matched.group(1) if market_matched else None
                                )
                                for review in entries:
                                    review["product_id"] = product_id
                                    review["marketplace"] = marketplace_string
                                    raw_data.append(review)
                        else:
                            logger.warning(f"Unexpected JSON structure in {file_path}")
                except Exception as e:
                    logger.error(f"Unexpected error with {file}: {e}")

    logger.info(f"Total records processed: {len(raw_data)}")
    return raw_data


def _clean_text(text):
    if pd.isna(text):
        return text
    # Ensure it's a string
    text = str(text)

    # Remove non-printable / non-UTF-8 characters
    text = text.encode("utf-8", "ignore").decode("utf-8", "ignore")

    # Remove control characters (ASCII < 32 except tab)
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)

    # Replace newlines, carriage returns, tabs with space
    text = re.sub(r"[\r\n\t]+", " ", text)

    # Collapse multiple spaces
    text = re.sub(r"\s{2,}", " ", text).strip()

    return text
