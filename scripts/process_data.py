# scripts/process_data.py

import argparse

from esf_pipeline.config import LOCAL_RAW_DIR
from esf_pipeline.process.cleaning import clean_product, clean_reviews
from esf_pipeline.process.standardise import (
    standardise_product_data,
    standardise_reviews,
)


def main():
    parser = argparse.ArgumentParser(description="Process product and review data.")
    parser.add_argument(
        "--task",
        choices=["products", "reviews", "all"],
        default="all",
        help="Task to perform: process products, reviews, or both.",
    )
    parser.add_argument(
        "--dir",
        type=str,
        help="Directory (relative to data/raw) containing the raw data.",
    )
    args = parser.parse_args()
    if args.task in ["products", "all"]:
        clean_product(standardise_product_data(LOCAL_RAW_DIR / args.dir))
    if args.task in ["reviews", "all"]:
        clean_reviews(standardise_reviews(LOCAL_RAW_DIR / args.dir))


if __name__ == "__main__":
    main()
