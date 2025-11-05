# src/esf_pipeline/scraper/common.py
"""Common utility functions for the ESF Scraper application."""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor


def check_query_list(query_list: list[str]) -> None:
    """Validates the query list."""
    if not query_list:
        raise ValueError("Query list cannot be empty")
    if not all(isinstance(q, str) and q.strip() for q in query_list):
        raise ValueError("All queries must be non-empty strings")


def retrieve_product_ids_from_query_list(
    query_list: list[str],
    id_retriever: Callable,
    maximum_products: int = 12,
    max_workers: int = 10,
) -> dict[list[str]]:
    """Retrieves product IDs from a list of search queries."""
    product_ids = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_query = {
            executor.submit(id_retriever, query): query for query in query_list
        }
        for future in future_to_query:
            result = future.result()
            product_ids.update(result)

    # Limit the number of product IDs to the maximum_products
    query_ids = {k: v[:maximum_products] for k, v in product_ids.items()}
    return query_ids


def get_data_from_query_ids(
    query_ids: dict[list[str]],
    getter_func: Callable,
    max_workers: int,
) -> list[dict]:
    """Retrieves data for each product ID using the provided getter function."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {
            executor.submit(getter_func, id): id
            for id_list in query_ids.values()
            for id in id_list
        }
        data = []
        for future in future_to_id:
            result = future.result()
            if result:
                entry = result
                if entry:
                    data.append(entry)
    return data


def match_product_query(query_ids: dict[list], products: list[dict]) -> list[dict]:
    """Matches products to their respective queries based on Product IDs."""
    id_to_query = {
        product_id: query
        for query, product_id_list in query_ids.items()
        for product_id in product_id_list
    }
    for product in products:
        product_id = product.get("product_id", None)
        if product_id and product_id in id_to_query:
            product["query"] = id_to_query[product_id]

    return products
