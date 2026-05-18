from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from app.channels.feishu_contract import ContractQualityResult, ContractValidationResult


@dataclass
class FeishuPresentationRetryFeedback:
    schema_errors: list[str]
    quality_errors: list[str]


def build_feishu_presentation_prompt(
    *,
    analysis_text: str,
    artifacts: list[str],
    attachments: list[dict[str, Any]],
    retry_feedback: FeishuPresentationRetryFeedback | None = None,
) -> str:
    payload: dict[str, Any] = {
        "task": "Generate a complete feishu_skill_contract for presenting the final analysis result.",
        "rules": [
            "Do not add new facts.",
            "Do not omit the core conclusions from the analysis text.",
            "Return metadata.feishu_skill_contract only.",
            "The contract must include title, summary, and multiple blocks when appropriate.",
            "At least one markdown conclusion block is required.",
            "Use chart blocks for structured numeric data when the analysis supports a chart.",
            "Do not include both chart and table blocks in the same card.",
            "When both are possible, prefer chart and omit table to avoid duplicate numeric presentation.",
            "Use image blocks only when image assets are explicitly available.",
            "Do not return plain explanation outside the contract payload.",
        ],
        "analysis_text": analysis_text,
        "artifacts": artifacts,
        "attachments": attachments,
        "expected_output_shape": {
            "metadata": {
                "feishu_skill_contract": {
                    "card_schema_version": "v1",
                    "target_channel": "feishu",
                    "render_mode": "card",
                    "fallback_text": "string",
                    "card_payload": {
                        "title": "string",
                        "summary": "string",
                        "blocks": [
                            {"type": "markdown", "markdown": "string"},
                            {
                                "type": "chart",
                                "chart": {
                                    "chart_type": "line|bar|pie|scatter|combo_bar_line",
                                    "title": "string",
                                    "why_this_chart": "string",
                                    "dimension": {"name": "string", "values": ["..."]},
                                    "metrics": [{"name": "string", "values": [1, 2]}],
                                },
                            },
                            {
                                "type": "table",
                                "table": {"title": "string", "columns": ["..."], "rows": [["..."]]},
                            },
                            {
                                "type": "image",
                                "image": {"image_key": "string", "caption": "string"},
                            },
                        ],
                    },
                }
            }
        },
    }
    if retry_feedback is not None:
        payload["retry_feedback"] = {
            "schema_errors": retry_feedback.schema_errors,
            "quality_errors": retry_feedback.quality_errors,
        }
        payload["rules"].append("Fix all retry_feedback issues in the regenerated contract.")

    return json.dumps(payload, ensure_ascii=False)


def build_feishu_presentation_retry_feedback(
    *,
    schema_result: ContractValidationResult | None,
    quality_result: ContractQualityResult | None,
) -> FeishuPresentationRetryFeedback:
    return FeishuPresentationRetryFeedback(
        schema_errors=list(schema_result.errors) if schema_result is not None else [],
        quality_errors=list(quality_result.errors) if quality_result is not None else [],
    )


def extract_feishu_skill_contract_candidate(result: dict | list | Mapping[str, Any] | None) -> dict[str, Any] | None:
    if isinstance(result, Mapping):
        metadata = result.get("metadata")
        if isinstance(metadata, Mapping):
            contract = metadata.get("feishu_skill_contract")
            if isinstance(contract, Mapping):
                return dict(contract)
        contract = result.get("feishu_skill_contract")
        if isinstance(contract, Mapping):
            return dict(contract)
    return None
