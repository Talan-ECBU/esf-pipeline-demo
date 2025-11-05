# src/esf_pipeline/process/standardise/text_search.py
"""Module to perform text search for electrical information."""

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

DASH = r"[\-\u2013\u2014]"

VOLT_PAT = re.compile(
    rf"""
    (?:\b(?P<pol>(?:ac|dc))\b[^\w]{{0,3}})?
    (?:
        (?P<v1>\d{{1,3}}(?:\.\d+)?)
        \s*(?P<vunit1>v|vac|vdc|\u2393)?
        \s*(?:{DASH}|/)
        \s*(?P<v2>\d{{1,3}}(?:\.\d+)?)
        \s*(?P<vunit2>v|vac|vdc|\u2393)?
      |
        (?P<v3>\d{{1,3}}(?:\.\d+)?)
        \s*(?P<vunit3>v|vac|vdc|\u2393)
      |
        (?:(?:rated\s+)?voltage)
        \s*[:\-]?\s*
        (?P<v4>\d{{1,4}}(?:\.\d+)?)
        \s*(?P<vunit4>v|vac|vdc|\u2393)?
      |
        ['"]?name['"]?\s*[:=]\s*['"]?voltage['"]?\s*,\s*['"]?value['"]?\s*[:=]\s*['"]?
        (?P<v5>\d{{1,4}}(?:\.\d+)?)
        \s*(?P<vunit5>v|vac|vdc|\u2393)?['"]?
    )
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

AMP_PAT = re.compile(
    rf"""
    (?P<a1>\d{{1,2}}(?:\.\d+)?)
    (?:\s*(?:{DASH}|/)\s*(?P<a2>\d{{1,2}}(?:\.\d+)?))?
    \s*(?P<aunit>ma|a)\b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

WATT_PAT = re.compile(
    rf"""
    (?P<w1>\d{{1,4}}(?:\.\d+)?)
    (?:\s*(?:{DASH}|/)\s*(?P<w2>\d{{1,4}}(?:\.\d+)?))?
    \s*(?P<wunit>kw|w)\b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

HZ_PAT = re.compile(
    rf"""
    (?P<h1>\d{{2,3}})
    (?:\s*(?:{DASH}|/)\s*(?P<h2>\d{{2,3}}))?
    \s*hz\b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

INPUT_CUES = re.compile(
    r"""
    \b(
        input
        | in
        | ac\s*in
        | dc\s*in
        | mains
        | supply
        | source
        | wall
        | cigarette\s*lighter
        | vehicle
        | car
        | truck
        | plug\s*in
    )\b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

OUTPUT_CUES = re.compile(
    r"""
    \b(
        output
        | out
        | port
        | charging
        | charge
        | usb(?:[-\s]?(?:c\d?|a\d?))
        | type[-\s]?c\d?
        | type[-\s]?a\d?
        | pd(?:\s*\d+(?:\.\d+)?)
        | pps
        | qc\s*\d(?:\.\d)?
    )\b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

AC_CUES = re.compile(
    r"""
    \b(
        ac
        | vac
        | 50/60\s*hz
        | hz
    )\b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

DC_CUES = re.compile(
    r"""
    \b(
        dc
        | usb
        | \u2393
    )\b
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

PORT_LABEL = re.compile(r"(?ix)\b(?:(usb)[-\s]?(c|a)\s*(\d+)?)\b|\b([CA])(\d)\b")

SPLIT_SENT = re.compile(rf"(?<=[\.\!\?;])\s+|\s*[•\u2022{DASH[1:-1]}]\s+")


def expand_electrical_info(
    df: pd.DataFrame,
    text_col: str,
) -> pd.DataFrame:
    if text_col not in df.columns:
        raise KeyError(f"{text_col!r} not in DataFrame")

    volts, amps, watts = [], [], []
    for s in df[text_col].fillna("").astype(str):
        v, a, w = _extract_contexted_specs(s)
        volts.append(v)
        amps.append(a)
        watts.append(w)

    out = df.copy()
    volts = pd.Series(volts, index=out.index)
    amps = pd.Series(amps, index=out.index)
    watts = pd.Series(watts, index=out.index)
    out["voltage_info"] = out["voltage_info"].combine(volts, _merge_dicts_nullable)
    has_voltage = out["voltage_info"].notna()
    logger.info(f"Final number of entries with voltage info: {has_voltage.sum()}")
    out["amperage_info"] = out["amperage_info"].combine(amps, _merge_dicts_nullable)
    has_amperage = out["amperage_info"].notna()
    logger.info(f"Final number of entries with amperage info: {has_amperage.sum()}")
    out["wattage_info"] = out["wattage_info"].combine(watts, _merge_dicts_nullable)
    has_wattage = out["wattage_info"].notna()
    logger.info(f"Final number of entries with wattage info: {has_wattage.sum()}")
    all_three = out[has_voltage & has_amperage & has_wattage]
    logger.info(f"Final number of entries with all three specs: {len(all_three)}")
    return out


def _window(text: str, start: int, end: int, width: int = 40) -> str:
    a = max(0, start - width)
    b = min(len(text), end + width)
    return text[a:b]


def _score_context(snippet: str) -> tuple[float, float, str | None]:
    """Return (input_score, output_score, acdc_hint)"""
    inp = 0.0
    out = 0.0
    acdc: str | None = None

    if INPUT_CUES.search(snippet):
        inp += 3.0
    if OUTPUT_CUES.search(snippet):
        out += 3.0
    if AC_CUES.search(snippet):
        inp += 1.5
        acdc = acdc or "AC"
    if DC_CUES.search(snippet):
        out += 1.0
        acdc = acdc or "DC"
    return inp, out, acdc


def _standard_context(base: str, port: str | None) -> str:
    base = base.lower()
    if port:
        return f"{base}:{port.upper()}"
    return base


def _nearest_port(snippet: str) -> str | None:
    # Prefer explicit like USB-C1, USB-A2, C1, A2, etc.
    m = PORT_LABEL.search(snippet)
    if not m:
        return None
    if m.group(1):  # 'usb' present
        kind = m.group(2) or ""
        idx = (m.group(3) or "").strip()
        label = f"USB-{kind.upper()}{idx}".strip()
    else:
        kind = m.group(4) or ""
        idx = m.group(5) or ""
        label = f"USB-{kind.upper()}{idx}".strip()
    return label


def _add_entry(bucket: dict[str, list[str]], key: str, value: str) -> None:
    if not value:
        return
    bucket.setdefault(key, [])
    if value not in bucket[key]:
        bucket[key].append(value)


def _fmt_a(a1: str, a2: str | None, unit: str) -> str:
    unit = unit.lower()
    if unit == "ma":
        # convert to A, preserve decimals
        val1 = str(round(float(a1) / 1000.0, 4))
        if a2:
            val2 = str(round(float(a2) / 1000.0, 4))
            return f"{val1}-{val2} A"
        return f"{val1} A"
    if a2:
        return f"{a1}-{a2} A"
    return f"{a1} A"


def _fmt_w(w1: str, w2: str | None, unit: str) -> str:
    unit = unit.lower()
    if unit == "kw":
        val1 = str(round(float(w1) * 1000.0, 3))
        if w2:
            val2 = str(round(float(w2) * 1000.0, 3))
            return f"{val1}-{val2} W"
        return f"{val1} W"
    if w2:
        return f"{w1}-{w2} W"
    return f"{w1} W"


def _extract_contexted_specs(
    text: str,
) -> tuple[dict[str, list[str]], dict[str, list[str]], dict[str, list[str]]]:
    """Extract and return (voltage_info, amperage_info, wattage_info)"""
    voltage: dict[str, list[str]] = {}
    amperage: dict[str, list[str]] = {}
    wattage: dict[str, list[str]] = {}

    # Split into approachable chunks (bullets/sentences)
    chunks = [c.strip() for c in SPLIT_SENT.split(text) if c and c.strip()] or [text]

    for ch in chunks:
        # Pre-scan ports for this chunk
        port = _nearest_port(ch)
        matches = _find_spec_matches(ch)

        for kind, m in matches:
            span_win = _window(ch, m.start(), m.end(), width=40)
            base = _extract_span_win(span_win, port)

            ctx_key = _standard_context(base, port)

            if kind == "V":
                if m.group("v1") and m.group("v2"):
                    v1, v2 = m.group("v1"), m.group("v2")
                    unit = (m.group("vunit1") or m.group("vunit2") or "V").upper()
                    value = f"{v1}-{v2} {unit}"
                elif m.group("v3"):
                    v = m.group("v3")
                    unit = (m.group("vunit3") or "V").upper()
                    value = f"{v} {unit}"
                elif m.group("v4"):
                    v = m.group("v4")
                    unit = (m.group("vunit4") or "V").upper()
                    value = f"{v} {unit}"
                elif m.group("v5"):
                    v = m.group("v5")
                    unit = (m.group("vunit5") or "V").upper()
                    value = f"{v} {unit}"
                else:
                    continue
                _add_entry(voltage, ctx_key, value)
            elif kind == "A":
                a1 = m.group("a1")
                a2 = m.group("a2")
                aunit = m.group("aunit") or "a"
                value = _fmt_a(a1, a2, aunit)
                _add_entry(amperage, ctx_key, value)
            elif kind == "W":
                w1 = m.group("w1")
                w2 = m.group("w2")
                wunit = m.group("wunit") or "w"
                value = _fmt_w(w1, w2, wunit)
                _add_entry(wattage, ctx_key, value)

    return voltage, amperage, wattage


def _find_spec_matches(ch: str) -> list:
    """Collect voltage/amperage/wattage regex matches with kind tags."""
    matches = []
    for m in VOLT_PAT.finditer(ch):
        matches.append(("V", m))
    for m in AMP_PAT.finditer(ch):
        matches.append(("A", m))
    for m in WATT_PAT.finditer(ch):
        matches.append(("W", m))
    return matches


def _extract_span_win(span_win: str, port: str) -> str:
    in_score, out_score, _ = _score_context(span_win)
    base = "unspecified"  # default
    if in_score > out_score:
        base = "input"
    elif out_score > in_score:
        base = "output"
    elif port:
        base = "output"
    elif HZ_PAT.search(span_win) or "VAC" in span_win.upper():
        base = "input"
    return base


def _merge_dicts_nullable(a, b):
    a = a if isinstance(a, dict) else {}
    b = b if isinstance(b, dict) else {}
    if not a and not b:
        return None
    return {**a, **b}
