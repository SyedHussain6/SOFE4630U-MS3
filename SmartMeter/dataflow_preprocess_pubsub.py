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
    Expected from MS1: a serialized dictionary message.
    Most students use JSON; some accidentally publish Python dict strings.
    We support both.
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


def extract_measurements(rec: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], str, str]:
    """
    Try common key names for pressure/temp (since MS1 implementations vary).
    Returns: (pressure_kpa, temp_c, pressure_key, temp_key)
    """
    pressure_keys = ["pressure", "Pressure", "pressure_kpa", "pressureKPa", "kpa", "Pressure(kPa)"]
    temp_keys = ["temperature", "Temperature", "temp", "temp_c", "tempC", "celsius", "Temperature(C)"]

    p_val = None
    p_key_used = ""
    for k in pressure_keys:
        if k in rec:
            p_val = _to_float(rec.get(k))
            p_key_used = k
            break

    t_val = None
    t_key_used = ""
    for k in temp_keys:
        if k in rec:
            t_val = _to_float(rec.get(k))
            t_key_used = k
            break

    return p_val, t_val, p_key_used, t_key_used


def preprocess_record(rec: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Stage 2 (Filter): drop missing pressure/temp
    Stage 3 (Convert): kPa->psi and C->F
    """
    pressure_kpa, temp_c, _, _ = extract_measurements(rec)

    # Filter stage
    if pressure_kpa is None or temp_c is None:
        return None

    # Convert stage (MS3 equations)
    pressure_psi = pressure_kpa / 6.895
    temp_f = temp_c * 1.8 + 32.0

    # Keep original fields + add converted fields
    out = dict(rec)
    out["pressure_kpa"] = pressure_kpa
    out["pressure_psi"] = round(pressure_psi, 4)
    out["temp_c"] = temp_c
    out["temp_f"] = round(temp_f, 4)

    return out


def to_pubsub_bytes(rec: Dict[str, Any]) -> bytes:
    return (json.dumps(rec) + "\n").encode("utf-8")


# -----------------------------
# Pipeline
# -----------------------------
def run(argv=None):
    parser = argparse.ArgumentParser()

    # Pub/Sub topics
    parser.add_argument("--input_topic", required=True, help="projects/<PROJECT>/topics/<RAW_TOPIC>")
    parser.add_argument("--output_topic", required=True, help="projects/<PROJECT>/topics/<PROCESSED_TOPIC>")

    # Optional but recommended for DataflowRunner
    parser.add_argument("--project", help="GCP project id")
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--temp_location", help="gs://<bucket>/temp")
    parser.add_argument("--staging_location", help="gs://<bucket>/staging")
    parser.add_argument("--runner", default="DirectRunner", help="DirectRunner or DataflowRunner")

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Force runner if user passed --runner
    pipeline_args.extend([f"--runner={known_args.runner}"])
    if known_args.project:
        pipeline_args.extend([f"--project={known_args.project}"])
    if known_args.region:
        pipeline_args.extend([f"--region={known_args.region}"])
    if known_args.temp_location:
        pipeline_args.extend([f"--temp_location={known_args.temp_location}"])
    if known_args.staging_location:
        pipeline_args.extend([f"--staging_location={known_args.staging_location}"])

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "ParseBytesToDict" >> beam.Map(parse_message)
            | "DropUnparseable" >> beam.Filter(lambda x: x is not None)
            | "FilterAndConvert" >> beam.Map(preprocess_record)
            | "DropMissingMeasurements" >> beam.Filter(lambda x: x is not None)
            | "DictToJSONBytes" >> beam.Map(to_pubsub_bytes)
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=known_args.output_topic)
        )


if __name__ == "__main__":
    run()
