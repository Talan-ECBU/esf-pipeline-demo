# src/esf_pipeline/process/processors/score_text.py
"""Module for scoring products via text-based criteria."""

import re
from collections.abc import Callable
from functools import partial
from logging import getLogger

import pandas as pd

from ...config.config import DATA_SCHEMA

logger = getLogger(__name__)

NUMBER_RANGE_RE = re.compile(
    r"""
    (?<![\w./])                                     # Not part of word/URL just before
    (?P<lo>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)        # First number (int/float/sci)
    (?:\s*(?:[a-zA-Z\u00B5\u03BC\u03A9\u00B0%]+))?  # Optional unit (µ, μ, Ω, °, %)
    (?:\s*(?:-|[\u2013\u2014])\s*                   # Hyphen, en dash, em dash
        (?P<hi>-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)
        (?:\s*(?:[a-zA-Z\u00B5\u03BC\u03A9\u00B0%]+))?  # Optional unit after hi
    )?                                                  # Second number is optional
    (?![\w./])                                          # Not followed by word/URL char
    """,
    re.VERBOSE,
)


def provide_compliance_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add compliance score columns to the dataframe based on electrical
    information.
    """
    logger.info("Providing basic compliance scores...")
    dfs = []
    for group, group_data in DATA_SCHEMA.items():
        raw_df = df[df["product_group"] == group].copy()
        exact_amperage = group_data.get("exact_amperage", False)
        exact_voltage = group_data.get("exact_voltage", False)
        raw_df = _provide_compliance_scores_to_groups(
            sub_df=raw_df, exact_amperage=exact_amperage, exact_voltage=exact_voltage
        )
        dfs.append(raw_df)

    result_df = pd.concat(dfs, ignore_index=True)

    products_with_v_score = result_df["voltage_score"].notna().sum()
    products_with_w_score = result_df["wattage_score"].notna().sum()
    products_with_a_score = result_df["amperage_score"].notna().sum()
    logger.info(f"Number of products with voltage scores: {products_with_v_score}")
    logger.info(f"Number of products with wattage scores: {products_with_w_score}")
    logger.info(f"Number of products with amperage scores: {products_with_a_score}")
    return result_df


def extract_numbers(s: str) -> tuple[float, float] | float | None:
    """
    Extract a number or number range from a string.

    Parameters
    ----------
    s : str
        Input string potentially containing a number or number range.

    Returns
    -------
    tuple[float, float] | float | None
        - Returns a float if a single number is found.
        - Returns a tuple (lo, hi) if a range is found.
        - Returns None if nothing is found.

    Examples
    --------
    >>> extract_numbers("0.21-1.34")
    (0.21, 1.34)
    >>> extract_numbers("Voltage: 5.0V")
    5.0
    >>> extract_numbers("no numbers here")
    None
    """
    m = NUMBER_RANGE_RE.search(s)
    if not m:
        return None
    lo = float(m.group("lo"))
    hi = m.group("hi")
    return (lo, float(hi)) if hi is not None else lo


def _provide_compliance_scores_to_groups(
    sub_df: pd.DataFrame, exact_amperage: bool, exact_voltage: bool
) -> pd.DataFrame:
    """Add compliance score columns to the sub dataframe."""
    df = sub_df.copy()
    check_volts = partial(_check_voltage, exact=exact_voltage)
    df["voltage_score"] = df["voltage_info"].apply(
        lambda x: _score_electrical_info(
            info=x,
            check_func=check_volts,
            abs_indicators=["input"],
            irrelevant_indicators=["output"],
            reset_on_compliance=True,
            set_leniency=True,
        )
    )
    check_amps = partial(_check_below_threshold, threshold=13.0, exact=exact_amperage)
    check_watts = partial(_check_below_threshold, threshold=3000.0, exact=False)
    df["amperage_score"] = df["amperage_info"].apply(
        lambda x: _score_electrical_info(
            info=x,
            check_func=check_amps,
            abs_indicators=["input", "output", "amperage", "current"],
            reset_on_compliance=True,
            set_leniency=True,
        )
    )
    df["wattage_score"] = df["wattage_info"].apply(
        lambda x: _score_electrical_info(
            info=x,
            check_func=check_watts,
            abs_indicators=["input", "output", "wattage", "power"],
            reset_on_compliance=False,
            set_leniency=False,
        )
    )
    return df


def _score_electrical_info(
    info: dict,
    check_func: Callable,
    abs_indicators: list[str] | None = None,
    irrelevant_indicators: list[str] | None = None,
    reset_on_compliance: bool = True,
    set_leniency: bool = True,
) -> float | None:
    """
    Generic scoring function for electrical information.


    Parameters
    ----------
    info : dict
        Dictionary containing electrical information.
    check_func : Callable
        Function to check compliance of a value.
    abs_indicators : list[str] | None, optional
        List of keywords indicating absolute non-compliance. The default is
        None.
    irrelevant_indicators : list[str] | None, optional
        List of keywords indicating irrelevant fields. The default is None.
    reset_on_compliance : bool, optional
        Whether to reset score to 0 on finding a compliant value. The default
        is True.
    set_leniency : bool, optional
        If True, any value in a list can be compliant for the context to be
        classified as compliant; if False, all must be compliant for context
        to be classified as compliant. The default is True.
    """

    if not isinstance(info, dict):
        return None

    abs_indicators = abs_indicators or []
    irrelevant_indicators = irrelevant_indicators or []

    final_score = 0.0

    for key, value in info.items():
        if isinstance(value, list) and set_leniency:
            is_compliant = any(check_func(v) for v in value)
        elif isinstance(value, list) and not set_leniency:
            is_compliant = all(check_func(v) for v in value)
        else:
            is_compliant = check_func(value)

        if any(ind in key.lower() for ind in irrelevant_indicators):
            continue

        elif not is_compliant:
            if any(ind in key.lower() for ind in abs_indicators):
                final_score += 1
            else:
                final_score += 0.5
        elif is_compliant:
            final_score = 0.0 if reset_on_compliance else final_score
            break

    return final_score


def _check_voltage(value: str, exact: bool = False) -> float:
    """Check if the voltage is compliant or within the compliant range."""
    lower_range = 230
    upper_range = 250

    exact_num = 230

    irrelevant_threshold = 100

    is_compliant = True

    if isinstance(value, str):
        num = extract_numbers(value)
        if isinstance(num, tuple):
            lo, hi = sorted(num)
            if exact and lo <= exact_num <= hi:
                is_compliant = True
            elif not exact and (lo >= lower_range and hi <= upper_range):
                is_compliant = True
            elif hi < irrelevant_threshold:
                is_compliant = True
            else:
                is_compliant = False

        elif isinstance(num, float):
            if (lower_range <= num <= upper_range) and not exact:
                is_compliant = True
            elif num == exact_num and exact:
                is_compliant = True
            elif num < irrelevant_threshold:
                is_compliant = True
            else:
                is_compliant = False

    return is_compliant


def _check_below_threshold(
    value: str, threshold: float, exact: bool = False
) -> float | None:
    """
    Check if the amperage or wattage described is below the specified
    threshold.
    """
    is_compliant = True
    irrelevant_threshold = 5  # For amperage
    if isinstance(value, str):
        num = extract_numbers(value)
        if isinstance(num, tuple) and not exact:
            _, hi = sorted(num)
            if hi <= threshold:
                is_compliant = True
            else:
                is_compliant = False
        elif isinstance(num, float):
            if num <= threshold and not exact:
                is_compliant = True
            elif num == threshold and exact:
                is_compliant = True
            elif num <= irrelevant_threshold and exact:
                is_compliant = True
            else:
                is_compliant = False

    return is_compliant
