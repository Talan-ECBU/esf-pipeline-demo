# src/esf_scraper/main.py
"""Main entrypoint to the ESF Scraper application."""

import base64
import importlib
import json
import logging
import os
import pkgutil
import re
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path

import yaml

from ..config.config import (
    LOCAL_RAW_DIR,
    MAX_NON_RENDER_WORKERS,
    PROJECT_ROOT,
    SCRAPE_DATE,
    setup_logging,
)
from . import marketplaces
from .conn import post_oxy
from .io import save_and_upload_images, save_and_upload_json

with open(PROJECT_ROOT / "data_schema.yaml") as f:
    PRODUCT_TARGETS = yaml.safe_load(f)

setup_logging()
logger = logging.getLogger(__name__)


def scrape_and_upload(upload_mode=False, download_images=False):
    """Dynamically runs the scrape functions for marketplaces."""
    package = marketplaces
    for test_group, parameters in PRODUCT_TARGETS.items():
        query_list = parameters.get("query_list", [])
        if not query_list:
            logger.warning(f"No queries for {test_group}. Skipping...")
            continue
        for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
            if is_pkg:
                continue  # skip subpackages if any
            full_module_name = f"{package.__name__}.{module_name}"
            module = importlib.import_module(full_module_name)
            scrape_func = getattr(module, "scrape", None)
            try:
                images_dir, products_path, reviews_path = _generate_paths(
                    marketplace=module_name, test_group=test_group
                )
                products, reviews = _scrape_data(
                    scrape_func=scrape_func,
                    query_list=query_list,
                    full_module_name=full_module_name,
                )

                if products is None:
                    logger.error(
                        f"Failed to extract products: {full_module_name}, {test_group}"
                    )
                    continue

                images = _get_image_urls(product_data=products)

                blob_dir = (
                    f"{SCRAPE_DATE}/{test_group}/{package}" if upload_mode else None
                )
                save_and_upload_json(
                    products,
                    str(products_path),
                    f"{blob_dir}/products.json" if blob_dir else None,
                )
                if reviews is not None:
                    save_and_upload_json(
                        reviews,
                        str(reviews_path),
                        f"{blob_dir}/reviews.json" if blob_dir else None,
                    )

                if download_images:
                    _download_images(
                        images, images_dir, blob_dir, download_mode="parallel"
                    )

            except Exception as e:
                logger.exception(f"Error in scraper {full_module_name}: {e}")


def download_scraped_images(
    root_dir: str, test_group: str = "SCHEMA", max_images: int | None = None
):
    """Downloads images for a specific product (test) group after scraping."""
    group_text_pattern = re.compile(r"(Group\s+[A-Z])", re.IGNORECASE)
    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".json") and "product" in file.lower():
                file_path = os.path.join(root, file)
                product_group = group_text_pattern.search(root).group()
                if test_group == "SCHEMA":
                    test_set = [
                        group
                        for group, params in PRODUCT_TARGETS.items()
                        if params.get("required_image", False)
                    ]
                    if product_group not in test_set:
                        continue
                elif test_group not in (product_group, "ALL"):
                    continue
                try:
                    with open(file_path, encoding="utf-8") as f:
                        product_data = json.load(f)
                        images = _get_image_urls(
                            product_data=product_data, max_images=max_images
                        )
                        images_dir = Path(root) / "images"
                        images_dir.mkdir(parents=True, exist_ok=True)
                        _download_images(images, images_dir, download_mode="parallel")
                except Exception as e:
                    logger.error(f"Unexpected error with {file}: {e}")


def _generate_paths(marketplace, test_group):
    base_dir = (
        LOCAL_RAW_DIR
        / f"date={SCRAPE_DATE}"
        / f"productgroup={test_group}"
        / f"marketplace={marketplace}"
    )
    images_dir = base_dir / "images"
    products_path = base_dir / "products.json"
    reviews_path = base_dir / "reviews.json"
    images_dir.mkdir(parents=True, exist_ok=True)
    base_dir.mkdir(parents=True, exist_ok=True)
    return images_dir, products_path, reviews_path


def _scrape_data(scrape_func, query_list: list, full_module_name: str):
    if callable(scrape_func):
        logger.debug(f"Running scraper for module: {full_module_name}")
        products, reviews = scrape_func(query_list)
    else:
        logger.warning(f"No callable 'scrape' function in module: {full_module_name}")
        products, reviews = None, None
    return products, reviews


def _get_image_urls(product_data: list, max_images: int | None = None) -> list:
    """Builds a list of dicts containing product ID and image URLs."""
    images = []
    for prod in product_data:
        if isinstance(prod, dict):
            urls = prod.get("images") or []
            if max_images is not None and isinstance(urls, list):
                urls = urls[:max_images]
        else:
            logger.warning("Wrong format detected for at least one product entry")
            continue
        count = len(urls)
        if count == 0:
            continue
        images.append({"product_id": prod["product_id"], "urls": urls})
    logger.debug(
        "_get_images result",
        extra={"input_count": len(product_data), "output_count": len(images)},
    )
    return images


def _download_images(
    images: list[dict],
    images_dir: Path,
    blob_dir: str | None = None,
    download_mode: str = "default",
) -> None:
    """Downloads images either sequentially or in parallel."""
    if download_mode == "default":
        logger.debug("Starting sequential image download...")
        for image in images:
            _scrape_image(image, images_dir, blob_dir)
    elif download_mode == "parallel":
        logger.debug("Starting parallel image download with ThreadPoolExecutor...")
        scrape_func = partial(_scrape_image, images_dir=images_dir, blob_dir=blob_dir)
        with ThreadPoolExecutor(max_workers=MAX_NON_RENDER_WORKERS) as executor:
            executor.map(scrape_func, images)


def _scrape_image(image_data: dict, images_dir: Path, blob_dir: str | None) -> None:
    pid = image_data["product_id"]
    urls = image_data.get("urls", [])
    for idx, url in enumerate(urls):
        try:
            payload = {
                "source": "universal",
                "url": url,
                "content_encoding": "base64",
            }
            logger.debug("Scraping image", extra={"product_id": pid, "url": url})
            result = post_oxy(payload)
            content_b64 = result["results"][0]["content"]
            image_bytes = base64.b64decode(content_b64)

            image_name = f"{pid}_image_{idx}.jpg"
            local_img = images_dir / image_name

            if blob_dir is None:
                img_blob = None
            else:
                img_blob = f"{blob_dir}/images/{pid}/{image_name}"

            save_and_upload_images(image_bytes, str(local_img), img_blob)

        except Exception:
            logger.exception(
                "Error scraping/uploading image", extra={"product_id": pid, "url": url}
            )
