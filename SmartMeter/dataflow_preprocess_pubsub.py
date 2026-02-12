#!/usr/bin/env python3


import argparse
import ast
import json
from typing import Any, Dict, Optional, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


# -----------------------------
# Helpers
# -----------------------------
def _to_float(x: Any) -> Optional[float]:
    """Convert values to float safely. Returns None if missing/invalid."""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip().lower()
        if s in {"", "none", "null", "nan"}:
            return None
        try:
            return float(x)
        except ValueError:
            return None
    return None


def parse_message(msg_bytes: bytes) -> Optional[Dict[str, Any]]:
    """
    Parse a Pub/Sub message into a dict.

    Supports both:
      - JSON: {"pressure": 101.3, "temperature": 25.0, ...}
      - Python dict string: {'pressure': 101.3, 'temperature': 25.0, ...}
    """
    try:
        s = msg_bytes.decode("utf-8").strip()
    except Exception:
        return None

    if not s:
        return None

    # Try JSON first
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    # Fallback: python dict string
    try:
        obj = ast.literal_eval(s)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    return None


def extract_measurements(rec: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract pressure (kPa) and temperature (C) from the record using common keys,
    since MS1 implementations can vary.
    """
    pressure_keys = [
        "pressure", "Pressure", "pressure_kpa", "pressureKPa", "kpa",
        "Pressure(kPa)", "pressure (kPa)", "PRESSURE"
    ]
    temp_keys = [
        "temperature", "Temperature", "temp", "temp_c", "tempC", "celsius",
        "Temperature(C)", "temperature (C)", "TEMP", "TEMP_C"
    ]

    pressure_kpa = None
    for k in pressure_keys:
        if k in rec:
            pressure_kpa = _to_float(rec.get(k))
            break

    temp_c = None
    for k in temp_keys:
        if k in rec:
            tem
