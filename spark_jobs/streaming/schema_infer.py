"""
spark_jobs/streaming/schema_infer.py
=====================================

Infers an IncomingSchema (dict[str, str]) from a batch of raw JSON messages
pulled from the vendor-feeds Pub/Sub topic.

Design goal: deliberately dependency-free
------------------------------------------
This module contains only pure Python — no PySpark, no google-cloud-pubsub.
That decision serves two purposes:

  1. Unit tests run instantly with zero infrastructure (no JVM, no emulator).
  2. The drift detector calls infer_schema_from_batch() with plain Python
     strings regardless of where the messages came from — keeping the
     inference logic fully decoupled from the transport layer.

Type inference priority (most specific → most general)
-------------------------------------------------------
  BOOLEAN > INT > BIGINT > DOUBLE > TIMESTAMP > STRING

When a field carries values of different types across messages, the types are
*merged* upward toward the most general common type:

  INT   + BIGINT  → BIGINT    (widening is safe)
  INT   + DOUBLE  → DOUBLE    (int fits in a double)
  BOOL  + INT     → STRING    (booleans and numbers share no safe common type)
  any   + STRING  → STRING    (STRING is the universal fallback)

Null / missing values
---------------------
  Null JSON values (Python None) and absent fields are skipped during type
  inference.  A field whose ALL values are null is assigned STRING as the
  safest default — it can hold any future value once data arrives.

Pub/Sub message format assumed
------------------------------
  Each Pub/Sub message data field is a UTF-8 encoded JSON object (not an array).
  Messages that are not valid JSON are logged and skipped.

Output
------
  Returns IncomingSchema = dict[str, str], compatible with
  services/drift_detector/schema_comparator.py :: compare_schemas().
"""

from __future__ import annotations

import json
import re
from typing import Any

import structlog

log = structlog.get_logger(__name__)

# IncomingSchema type alias — mirrors the definition in schema_comparator.py.
# Redefined here to keep this module import-free from the services package.
IncomingSchema = dict[str, str]

# ---------------------------------------------------------------------------
# Type constants
# ---------------------------------------------------------------------------

_BOOLEAN = "BOOLEAN"
_INT = "INT"
_BIGINT = "BIGINT"
_DOUBLE = "DOUBLE"
_TIMESTAMP = "TIMESTAMP"
_STRING = "STRING"

# Ordered from most specific to most general for numeric types.
# BOOLEAN is deliberately excluded from this chain — it is incompatible
# with numeric types and collapses to STRING when mixed with them.
_NUMERIC_CHAIN: list[str] = [_INT, _BIGINT, _DOUBLE]

# INT fits in 32-bit signed range.
_INT32_MIN = -(2**31)
_INT32_MAX = 2**31 - 1

# BIGINT fits in 64-bit signed range.
_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

# ISO 8601 timestamp patterns — covers the most common vendor formats:
#   "2024-01-15T10:30:00Z"
#   "2024-01-15T10:30:00+05:30"
#   "2024-01-15 10:30:00"
#   "2024-01-15" (date-only)
_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}"           # date part: YYYY-MM-DD (required)
    r"(?:[T ]\d{2}:\d{2}:\d{2}"     # optional time: T or space + HH:MM:SS
    r"(?:\.\d+)?"                    # optional fractional seconds
    r"(?:Z|[+-]\d{2}:?\d{2})?)?$",  # optional timezone (Z or ±HH:MM)
    re.ASCII,
)


# ---------------------------------------------------------------------------
# Single-value type inference
# ---------------------------------------------------------------------------


def _infer_value_type(value: Any) -> str:
    """
    Return the most specific type string for a single Python value.

    Python's json.loads() produces:
      JSON true/false  → Python bool
      JSON integers    → Python int  (arbitrary precision)
      JSON floats      → Python float (64-bit double)
      JSON strings     → Python str
      JSON null        → Python None

    Important: bool is a subclass of int in Python, so the isinstance(value, bool)
    check MUST come before isinstance(value, int) — otherwise True/False would
    be classified as INT.
    """
    if value is None:
        # None means "no information" — caller skips these
        return "NULL"  # sentinel; never written to the final schema

    if isinstance(value, bool):
        # Booleans are their own type — not coercible to INT without loss of meaning
        return _BOOLEAN

    if isinstance(value, int):
        # Python int is arbitrary precision; map to SQL INT or BIGINT by range
        if _INT32_MIN <= value <= _INT32_MAX:
            return _INT
        if _INT64_MIN <= value <= _INT64_MAX:
            return _BIGINT
        # Integers outside 64-bit range are extremely rare in vendor feeds;
        # STRING is the safest fallback
        return _STRING

    if isinstance(value, float):
        # JSON has no float/double distinction — Python always gives us a
        # 64-bit double, so we map directly to DOUBLE
        return _DOUBLE

    if isinstance(value, str):
        if _TIMESTAMP_RE.match(value):
            return _TIMESTAMP
        return _STRING

    # dict, list, or any other complex type → STRING (treat as opaque blob)
    return _STRING


# ---------------------------------------------------------------------------
# Type merging — determines the common type when a field has mixed values
# ---------------------------------------------------------------------------


def _merge_types(a: str, b: str) -> str:
    """
    Return the most general type that can safely hold values of both types.

    This is called once per field per message to accumulate the field's
    running type across the whole batch.

    Rules (in priority order)
    -------------------------
    Same type          → return it unchanged (fast path).
    NULL + anything    → return the non-NULL type (nulls give no type info).
    BOOLEAN + numeric  → STRING (no safe common parent in SQL type systems).
    Numeric widening   → follow the INT → BIGINT → DOUBLE chain.
    TIMESTAMP + STRING → STRING (a timestamp can always be stored as a string).
    any + STRING       → STRING (universal fallback — nothing is lost).
    incompatible       → STRING (conservative default for all other pairs).
    """
    if a == b:
        return a  # most common case — avoid all further checks

    # NULL is a sentinel meaning "no type observed yet" — the other type wins
    if a == "NULL":
        return b
    if b == "NULL":
        return a

    # BOOLEAN is only compatible with itself (already handled) and STRING
    if _BOOLEAN in (a, b):
        return _STRING

    # Numeric widening: INT → BIGINT → DOUBLE
    if a in _NUMERIC_CHAIN and b in _NUMERIC_CHAIN:
        # Return the type that appears later (= more general) in the chain
        return _NUMERIC_CHAIN[max(_NUMERIC_CHAIN.index(a), _NUMERIC_CHAIN.index(b))]

    # TIMESTAMP mixed with anything other than itself → STRING.
    # A timestamp is a semantic type, not a raw data type — mixing it with
    # STRING means we can't guarantee the format, so STRING is safer.
    if _TIMESTAMP in (a, b):
        return _STRING

    # Default: STRING absorbs everything
    return _STRING


# ---------------------------------------------------------------------------
# Batch-level inference — the public API
# ---------------------------------------------------------------------------


def infer_schema_from_batch(messages: list[str]) -> IncomingSchema:
    """
    Infer a flat field→type schema from a list of raw JSON message strings.

    Parameters
    ----------
    messages : list[str]
        Raw UTF-8 JSON strings from Pub/Sub, one per message.
        Each message must be a JSON object (dict), not an array or scalar.

    Returns
    -------
    IncomingSchema
        Mapping of field name → upper-cased type string.
        Compatible with schema_comparator.compare_schemas() as the
        ``incoming`` parameter.

    Behaviour for edge cases
    ------------------------
    - Non-JSON messages are logged as warnings and skipped.
    - Messages that are JSON arrays or scalars (not objects) are skipped.
    - Fields whose values are null in every message default to STRING.
    - An empty message list returns an empty dict (no schema inferred).
    - Nested objects / arrays within a field are typed as STRING.

    Example
    -------
        messages = [
            '{"user_id": 1001, "name": "Alice", "score": 9.5}',
            '{"user_id": 1002, "name": "Bob",   "score": 8.1}',
            '{"user_id": 1003, "name": null,    "score": 7.3, "new_flag": true}',
        ]
        schema = infer_schema_from_batch(messages)
        # → {"user_id": "INT", "name": "STRING", "score": "DOUBLE", "new_flag": "BOOLEAN"}
    """
    if not messages:
        return {}

    # running_types accumulates the merged type for each field across all messages.
    # Initial value is "NULL" (sentinel) — the first real value sets the type.
    running_types: dict[str, str] = {}

    parsed_count = 0

    for raw in messages:
        try:
            record = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            log.warning("schema_infer.skip_invalid_json", raw_preview=raw[:120])
            continue

        if not isinstance(record, dict):
            # Arrays and scalars are not valid vendor feed messages
            log.warning("schema_infer.skip_non_object", type=type(record).__name__)
            continue

        parsed_count += 1

        for field_name, value in record.items():
            inferred = _infer_value_type(value)
            if field_name not in running_types:
                # First time we've seen this field — initialise with its type
                running_types[field_name] = inferred
            else:
                # Merge the new observation into the accumulated type
                running_types[field_name] = _merge_types(
                    running_types[field_name], inferred
                )

    log.info(
        "schema_infer.complete",
        total_messages=len(messages),
        parsed=parsed_count,
        skipped=len(messages) - parsed_count,
        fields_inferred=len(running_types),
    )

    # Any field that was only ever observed as NULL gets STRING as the fallback.
    # This can happen when a field is present in every message but always null.
    final_schema: IncomingSchema = {
        name: (_STRING if ftype == "NULL" else ftype)
        for name, ftype in running_types.items()
    }

    return final_schema


def infer_schema_from_pubsub_messages(raw_data_list: list[bytes]) -> IncomingSchema:
    """
    Adapter for messages pulled from a Pub/Sub subscription.

    Pub/Sub delivers each message as a ``ReceivedMessage`` object. The caller
    extracts the raw bytes from ``message.data`` and passes them here as a
    list — keeping this function free of any google-cloud-pubsub dependency
    so it remains unit-testable without a running emulator.

    Parameters
    ----------
    raw_data_list : list[bytes]
        The ``.data`` bytes field from each Pub/Sub ReceivedMessage.
        Each element must be a UTF-8 encoded JSON object string.

    Returns
    -------
    IncomingSchema
        Result of infer_schema_from_batch() on the decoded messages.

    Example (inside the drift detector subscriber loop)
    ----------------------------------------------------
        response = subscriber.pull(subscription=sub_path, max_messages=1000)
        raw_data = [msg.message.data for msg in response.received_messages]
        schema   = infer_schema_from_pubsub_messages(raw_data)
    """
    strings: list[str] = []
    for data in raw_data_list:
        try:
            strings.append(data.decode("utf-8"))
        except (UnicodeDecodeError, AttributeError):
            log.warning("schema_infer.skip_non_utf8_pubsub_message")

    return infer_schema_from_batch(strings)


# ---------------------------------------------------------------------------
# Demo / manual test entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json as _json

    sample_messages = [
        _json.dumps({"order_id": 1001, "customer": "Alice",   "amount": 299.99,  "active": True,  "ts": "2024-01-15T10:30:00Z"}),
        _json.dumps({"order_id": 1002, "customer": "Bob",     "amount": 149.0,   "active": False, "ts": "2024-01-16T08:00:00Z"}),
        _json.dumps({"order_id": 1003, "customer": None,      "amount": 89.5,    "active": True,  "ts": "2024-01-17T12:00:00Z", "discount": 10}),
        _json.dumps({"order_id": 1004, "customer": "Charlie", "amount": 9999999999, "active": False, "ts": "2024-01-18T09:15:00Z"}),
    ]

    print("Input messages:")
    for m in sample_messages:
        print(f"  {m}")

    schema = infer_schema_from_batch(sample_messages)

    print("\nInferred schema:")
    for field, ftype in sorted(schema.items()):
        print(f"  {field:<20} → {ftype}")

    # Expected output:
    #   active               → BOOLEAN
    #   amount               → DOUBLE
    #   customer             → STRING
    #   discount             → INT
    #   order_id             → INT   (1001–1003 fit INT; 1004 = 9999999999 > INT32_MAX → BIGINT after merge)
    #   ts                   → TIMESTAMP
