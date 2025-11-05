# src/esf_pipeline/scraper/conn.py
"""Connection functions for external services like Oxylabs scraping API."""

import os

import requests
from dotenv import load_dotenv

from .io import save_json

load_dotenv()
USER = os.getenv("OYX_USERNAME")
PASSWORD = os.getenv("OYX_PASSWORD")


def post_oxy(payload: dict, debug: bool = False) -> dict:
    """Sends a POST request to the Oxylabs Realtime API with the given payload."""
    kwargs = {"auth": (USER, PASSWORD), "json": payload}

    response = requests.request(
        "POST", "https://realtime.oxylabs.io/v1/queries", **kwargs
    )
    if debug:
        save_json(response.json(), "last_request.json")

    return response.json()


def get_search_content(payload):
    """Retrieves search content from Oxylabs Realtime API."""
    data = post_oxy(payload)
    if not isinstance(data, dict):
        raise RuntimeError("Response was not a dictionary")

    results = data.get("results")
    if not results or not isinstance(results, list) or not isinstance(results[0], dict):
        raise RuntimeError("Missing or malformed 'results'")

    content = results[0].get("content")
    if content is None:
        raise RuntimeError("Missing 'content' in result")
    return content
