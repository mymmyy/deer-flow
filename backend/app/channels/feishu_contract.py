"""Feishu card contract (v1) validation and normalization.

This module is intentionally independent from Feishu sending code so we can
stabilize model output contract first, then wire it into renderer/channel.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

CONTRACT_VERSION = "v1"
TARGET_CHANNEL = "feishu"
RENDER_MODES = {"text", "card"}
BLOCK_TYPES = {"markdown", "table", "chart"}
CHART_TYPES = {"line", "bar", "pie", "scatter", "combo_bar_line"}
COMBO_SERIES_TYPES = {"bar", "line"}
COMBO_Y_AXIS = {"left", "right"}
DEFAULT_FALLBACK_TEXT = "Card rendering is unavailable. Please check the text response."


@dataclass
class ContractValidationResult:
    ok: bool
    normalized: dict[str, Any] | None
    errors: list[str]


def _as_str(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _normalize_number(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        raw = value.strip().replace(",", "")
        if not raw:
            return None
        try:
            number = float(raw)
        except ValueError:
            return None
        if number.is_integer():
            return int(number)
        return number
    return None


def _normalize_markdown_block(block: dict[str, Any], errors: list[str], index: int) -> dict[str, Any] | None:
    markdown = _as_str(block.get("markdown")).strip()
    if not markdown:
        errors.append(f"blocks[{index}].markdown must be a non-empty string")
        return None
    return {"type": "markdown", "markdown": markdown}


def _normalize_table_block(block: dict[str, Any], errors: list[str], index: int) -> dict[str, Any] | None:
    table = _as_dict(block.get("table"))
    if not table:
        errors.append(f"blocks[{index}].table must be an object")
        return None

    title = _as_str(table.get("title")).strip()
    columns_raw = _as_list(table.get("columns"))
    rows_raw = _as_list(table.get("rows"))

    if not columns_raw or not all(isinstance(col, str) and col.strip() for col in columns_raw):
        errors.append(f"blocks[{index}].table.columns must be non-empty string[]")
        return None
    if not isinstance(rows_raw, list) or not all(isinstance(row, list) for row in rows_raw):
        errors.append(f"blocks[{index}].table.rows must be array of rows")
        return None

    columns = [str(col).strip() for col in columns_raw]
    normalized_rows: list[list[str]] = []
    for row_idx, row in enumerate(rows_raw):
        values = [str(cell) for cell in row[: len(columns)]]
        if len(values) < len(columns):
            values.extend([""] * (len(columns) - len(values)))
        if len(values) != len(columns):
            errors.append(f"blocks[{index}].table.rows[{row_idx}] has invalid width")
            return None
        normalized_rows.append(values)

    normalized: dict[str, Any] = {
        "type": "table",
        "table": {
            "columns": columns,
            "rows": normalized_rows,
        },
    }
    if title:
        normalized["table"]["title"] = title
    return normalized


def _normalize_chart_block(block: dict[str, Any], errors: list[str], index: int) -> dict[str, Any] | None:
    chart = _as_dict(block.get("chart"))
    if not chart:
        errors.append(f"blocks[{index}].chart must be an object")
        return None

    chart_type = _as_str(chart.get("chart_type")).strip().lower()
    if chart_type not in CHART_TYPES:
        errors.append(f"blocks[{index}].chart.chart_type must be one of {sorted(CHART_TYPES)}")
        return None

    title = _as_str(chart.get("title")).strip()
    dimension = _as_dict(chart.get("dimension"))
    dim_name = _as_str(dimension.get("name")).strip()
    dim_values_raw = _as_list(dimension.get("values"))
    if not dim_name:
        errors.append(f"blocks[{index}].chart.dimension.name must be a non-empty string")
        return None
    if not dim_values_raw:
        errors.append(f"blocks[{index}].chart.dimension.values must be a non-empty array")
        return None
    dim_values = [str(v) for v in dim_values_raw]

    metrics_raw = _as_list(chart.get("metrics"))
    if not metrics_raw or not all(isinstance(m, Mapping) for m in metrics_raw):
        errors.append(f"blocks[{index}].chart.metrics must be an array of objects")
        return None

    normalized_metrics: list[dict[str, Any]] = []
    for metric_idx, metric_raw in enumerate(metrics_raw):
        metric = dict(metric_raw)
        name = _as_str(metric.get("name")).strip()
        if not name:
            errors.append(f"blocks[{index}].chart.metrics[{metric_idx}].name must be a non-empty string")
            return None

        values_raw = _as_list(metric.get("values"))
        if len(values_raw) != len(dim_values):
            errors.append(
                f"blocks[{index}].chart.metrics[{metric_idx}].values length must match dimension.values length"
            )
            return None
        values: list[int | float] = []
        for v_idx, value in enumerate(values_raw):
            number = _normalize_number(value)
            if number is None:
                errors.append(
                    f"blocks[{index}].chart.metrics[{metric_idx}].values[{v_idx}] must be numeric"
                )
                return None
            values.append(number)

        normalized_metric: dict[str, Any] = {"name": name, "values": values}
        if chart_type == "combo_bar_line":
            series_type = _as_str(metric.get("series_type")).strip().lower()
            if series_type not in COMBO_SERIES_TYPES:
                errors.append(
                    f"blocks[{index}].chart.metrics[{metric_idx}].series_type must be one of {sorted(COMBO_SERIES_TYPES)}"
                )
                return None
            y_axis = _as_str(metric.get("y_axis")).strip().lower() or "left"
            if y_axis not in COMBO_Y_AXIS:
                errors.append(
                    f"blocks[{index}].chart.metrics[{metric_idx}].y_axis must be one of {sorted(COMBO_Y_AXIS)}"
                )
                return None
            normalized_metric["series_type"] = series_type
            normalized_metric["y_axis"] = y_axis
        normalized_metrics.append(normalized_metric)

    if chart_type == "pie" and len(normalized_metrics) != 1:
        errors.append(f"blocks[{index}].chart pie requires exactly 1 metric")
        return None
    if chart_type == "scatter" and len(normalized_metrics) != 2:
        errors.append(f"blocks[{index}].chart scatter requires exactly 2 metrics")
        return None
    if chart_type == "combo_bar_line" and len(normalized_metrics) < 2:
        errors.append(f"blocks[{index}].chart combo_bar_line requires at least 2 metrics")
        return None

    normalized_chart: dict[str, Any] = {
        "chart_type": chart_type,
        "dimension": {"name": dim_name, "values": dim_values},
        "metrics": normalized_metrics,
    }
    if title:
        normalized_chart["title"] = title

    return {"type": "chart", "chart": normalized_chart}


def validate_and_normalize_contract(
    raw: Any,
    *,
    channel_name: str | None = None,
) -> ContractValidationResult:
    errors: list[str] = []

    if not isinstance(raw, Mapping):
        return ContractValidationResult(ok=False, normalized=None, errors=["contract must be an object"])
    source = dict(raw)

    version = _as_str(source.get("card_schema_version")).strip()
    if version != CONTRACT_VERSION:
        errors.append(f"card_schema_version must be '{CONTRACT_VERSION}'")

    target_channel = _as_str(source.get("target_channel")).strip().lower()
    if target_channel != TARGET_CHANNEL:
        errors.append(f"target_channel must be '{TARGET_CHANNEL}'")

    if channel_name is not None and str(channel_name).strip().lower() != TARGET_CHANNEL:
        errors.append("contract is only valid for feishu channel")

    render_mode = _as_str(source.get("render_mode")).strip().lower()
    if render_mode not in RENDER_MODES:
        errors.append(f"render_mode must be one of {sorted(RENDER_MODES)}")

    fallback_text = _as_str(source.get("fallback_text")).strip() or DEFAULT_FALLBACK_TEXT

    normalized: dict[str, Any] = {
        "card_schema_version": CONTRACT_VERSION,
        "target_channel": TARGET_CHANNEL,
        "render_mode": render_mode if render_mode in RENDER_MODES else "text",
        "fallback_text": fallback_text,
    }

    if normalized["render_mode"] == "card":
        card_payload = _as_dict(source.get("card_payload"))
        if not card_payload:
            errors.append("card_payload must be an object when render_mode='card'")
        else:
            title = _as_str(card_payload.get("title")).strip()
            summary = _as_str(card_payload.get("summary")).strip()
            blocks_raw = _as_list(card_payload.get("blocks"))
            if not blocks_raw:
                errors.append("card_payload.blocks must be a non-empty array")
            else:
                normalized_blocks: list[dict[str, Any]] = []
                for idx, block_raw in enumerate(blocks_raw):
                    if not isinstance(block_raw, Mapping):
                        errors.append(f"blocks[{idx}] must be an object")
                        continue
                    block = dict(block_raw)
                    block_type = _as_str(block.get("type")).strip().lower()
                    if block_type not in BLOCK_TYPES:
                        errors.append(f"blocks[{idx}].type must be one of {sorted(BLOCK_TYPES)}")
                        continue

                    normalized_block: dict[str, Any] | None = None
                    if block_type == "markdown":
                        normalized_block = _normalize_markdown_block(block, errors, idx)
                    elif block_type == "table":
                        normalized_block = _normalize_table_block(block, errors, idx)
                    elif block_type == "chart":
                        normalized_block = _normalize_chart_block(block, errors, idx)

                    if normalized_block is not None:
                        normalized_blocks.append(normalized_block)

                if normalized_blocks:
                    normalized_payload: dict[str, Any] = {"blocks": normalized_blocks}
                    if title:
                        normalized_payload["title"] = title
                    if summary:
                        normalized_payload["summary"] = summary
                    normalized["card_payload"] = normalized_payload

    return ContractValidationResult(ok=not errors, normalized=normalized if not errors else None, errors=errors)

