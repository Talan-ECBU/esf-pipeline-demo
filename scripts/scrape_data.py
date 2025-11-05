# scripts/scrape_data.py
"""Scripts to execute the esf_scraper package."""

import argparse

from esf_pipeline.config import LOCAL_RAW_DIR
from esf_pipeline.scraper import download_scraped_images, scrape_and_upload


def main():
    parser = argparse.ArgumentParser(description="Run the ESF Scraper")
    parser.add_argument(
        "--products", action="store_true", help="Scrape and upload products"
    )
    parser.add_argument(
        "--images", action="store_true", help="Download from scraped image urls"
    )
    parser.add_argument(
        "--root-dir",
        type=str,
        default=".",
        help="Root directory (relative to data/raw) with scraping results",
    )
    parser.add_argument(
        "--max-images", type=int, default=5, help="Max images per product"
    )
    parser.add_argument("--upload", action="store_true", help="Upload mode")
    args = parser.parse_args()
    root_dir = LOCAL_RAW_DIR / args.root_dir
    if args.products:
        scrape_and_upload(upload_mode=args.upload, download_images=False)
    elif args.images:
        download_scraped_images(root_dir=root_dir, max_images=args.max_images)


if __name__ == "__main__":
    main()
