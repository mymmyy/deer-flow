"""Feishu/Lark channel - connects to Feishu via WebSocket (no public IP needed)."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
from typing import Any, Literal, Mapping

from app.channels.base import Channel
from app.channels.commands import KNOWN_CHANNEL_COMMANDS
from app.channels.feishu_contract import validate_and_normalize_contract
from app.channels.message_bus import InboundMessage, InboundMessageType, MessageBus, OutboundMessage, ResolvedAttachment
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, get_paths
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.sandbox.sandbox_provider import get_sandbox_provider

logger = logging.getLogger(__name__)

_ALLOWED_CONTEXT_BOUNDARIES = {"chat", "group", "topic"}
_ALLOWED_CARD_STYLES = {"markdown", "rich"}
_ALLOWED_RENDER_MODES = {"auto", "card", "text"}
_FEISHU_GROUP_CHAT_TYPES = {"group"}
_FEISHU_TEXT_MENTION_RE = re.compile(r"@_user_\d+\b")
_SCRIPT_PERMISSION_QUERY_MARKERS = (
    "是否允许我执行该脚本",
    "允许执行脚本",
    "执行脚本",
    "允许我执行",
    "allow me to execute",
    "allow script execution",
)
_FEISHU_CONTRACT_GUARDRAIL_TEXT = (
    "该请求需要以飞书卡片契约输出图表，已禁止脚本执行链路。"
    "请直接返回 `feishu_skill_contract` 或 `feishu_card_payload.chart_spec` 对应的卡片结果；"
    "若暂不可用，先返回文本趋势摘要。"
)


_MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\]]*]\([^)]+\)")
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```", re.IGNORECASE)
_MENU_EVENT_KEY_TO_COMMAND: dict[str, str] = {
    "model:gpt-5.2": "/model gpt-5.2",
    "model:qwen3.5-plus": "/model qwen3.5-plus",
    "mode:flash": "/preset flash",
    "mode:thinking": "/preset thinking",
    "mode:pro": "/preset pro",
    "mode:ultra": "/preset ultra",
    "mode:plan_on": "/mode plan on",
    "mode:plan_off": "/mode plan off",
    "mode:subagent_on": "/mode subagent on",
    "mode:subagent_off": "/mode subagent off",
    "mode:reasoning_low": "/mode reasoning low",
    "mode:reasoning_medium": "/mode reasoning medium",
    "mode:reasoning_high": "/mode reasoning high",
    "mode:reasoning_default": "/mode reasoning default",
    "session:reset": "/session reset",
}


def _normalize_menu_event_mapping(raw: Any) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        return dict(_MENU_EVENT_KEY_TO_COMMAND)
    normalized: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str) or not isinstance(v, str):
            continue
        key = k.strip().lower()
        value = v.strip()
        if key and value:
            normalized[key] = value
    return normalized or dict(_MENU_EVENT_KEY_TO_COMMAND)


def _safe_get(source: Any, key: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(key, default)
    return getattr(source, key, default)


def _first_non_empty_str(*values: Any) -> str:
    for value in values:
        if isinstance(value, str):
            text = value.strip()
            if text:
                return text
    return ""


def _path_get(source: Any, path: str) -> Any:
    current = source
    for part in path.split("."):
        if current is None:
            return None
        current = _safe_get(current, part)
    return current


def _sanitize_for_log(value: Any) -> Any:
    """Best-effort event payload sanitizer for diagnostics."""
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, inner in value.items():
            key_str = str(key)
            lowered = key_str.lower()
            if any(token in lowered for token in ("token", "secret", "sign", "access_key", "ticket")):
                sanitized[key_str] = "***"
            else:
                sanitized[key_str] = _sanitize_for_log(inner)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_for_log(item) for item in value[:20]]
    if isinstance(value, tuple):
        return tuple(_sanitize_for_log(item) for item in value[:20])
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if hasattr(value, "__dict__"):
        return _sanitize_for_log(vars(value))
    return str(value)


def _sanitize_markdown_for_feishu_card(content: str) -> str:
    """Strip markdown image syntax that Feishu rejects without image_key."""
    if not content:
        return content
    sanitized = _MARKDOWN_IMAGE_RE.sub("[image omitted]", content)
    return sanitized


def _resolve_receive_id_type(receive_id: str) -> str:
    value = str(receive_id or "").strip()
    return "open_id" if value.startswith("ou_") else "chat_id"


def _extract_json_mappings_from_text(text: str) -> list[dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return []

    candidates: list[str] = [raw]
    for match in _JSON_FENCE_RE.finditer(raw):
        block = match.group(1).strip()
        if block:
            candidates.append(block)

    decoder = json.JSONDecoder()
    mappings: list[dict[str, Any]] = []
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, Mapping):
                mappings.append(dict(parsed))
            continue
        except Exception:
            pass

        idx = 0
        while idx < len(candidate):
            if candidate[idx] != "{":
                idx += 1
                continue
            try:
                parsed_obj, end = decoder.raw_decode(candidate[idx:])
            except Exception:
                idx += 1
                continue
            if isinstance(parsed_obj, Mapping):
                mappings.append(dict(parsed_obj))
            idx += max(end, 1)
    return mappings


def _extract_embedded_feishu_payloads(text: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    for parsed in _extract_json_mappings_from_text(text):
        direct_contract = parsed.get("feishu_skill_contract")
        if isinstance(direct_contract, Mapping):
            return dict(direct_contract), None
        direct_payload = parsed.get("feishu_card_payload")
        if isinstance(direct_payload, Mapping):
            return None, dict(direct_payload)

        metadata = parsed.get("metadata")
        if isinstance(metadata, Mapping):
            nested_contract = metadata.get("feishu_skill_contract")
            if isinstance(nested_contract, Mapping):
                return dict(nested_contract), None
            nested_payload = metadata.get("feishu_card_payload")
            if isinstance(nested_payload, Mapping):
                return None, dict(nested_payload)
    return None, None


def _contract_summary(contract: Mapping[str, Any]) -> str:
    version = str(contract.get("card_schema_version") or "")
    target = str(contract.get("target_channel") or "")
    mode = str(contract.get("render_mode") or "")
    return f"version={version} target={target} mode={mode}"


def _format_elapsed_duration(raw_seconds: Any) -> str:
    try:
        total = int(raw_seconds)
    except (TypeError, ValueError):
        total = 0
    total = max(0, total)
    hours = total // 3600
    minutes = (total % 3600) // 60
    seconds = total % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def _is_feishu_command(text: str) -> bool:
    if not text.startswith("/"):
        return False
    return text.split(maxsplit=1)[0].lower() in KNOWN_CHANNEL_COMMANDS


def _apply_script_permission_guardrail(text: str) -> str:
    lowered = text.lower()
    if any(marker.lower() in lowered for marker in _SCRIPT_PERMISSION_QUERY_MARKERS):
        return _FEISHU_CONTRACT_GUARDRAIL_TEXT
    return text


class FeishuChannel(Channel):
    """Feishu/Lark IM channel using the ``lark-oapi`` WebSocket client.

    Configuration keys (in ``config.yaml`` under ``channels.feishu``):
        - ``app_id``: Feishu app ID.
        - ``app_secret``: Feishu app secret.
        - ``verification_token``: (optional) Event verification token.

    The channel uses WebSocket long-connection mode so no public IP is required.

    Message flow:
        1. User sends a message; bot adds "OK" emoji reaction
        2. Bot replies in thread: "Working on it......"
        3. Agent processes the message and returns a result
        4. Bot replies in thread with the result
        5. Bot adds "DONE" emoji reaction to the original message
    """

    def __init__(self, bus: MessageBus, config: dict[str, Any]) -> None:
        super().__init__(name="feishu", bus=bus, config=config)
        self._context_boundary = self._normalize_context_boundary(config.get("context_boundary", "chat"))
        self._reply_in_thread = self._parse_bool(config.get("reply_in_thread"), default=False)
        self._card_style = self._normalize_card_style(config.get("card_style", "rich"))
        self._render_mode = self._normalize_render_mode(config.get("render_mode", "auto"))
        self._require_mention_in_group = self._parse_bool(config.get("require_mention_in_group"), default=True)
        self._bot_open_id = str(config.get("bot_open_id") or "").strip()
        self._bot_name = str(config.get("bot_name") or "").strip().lstrip("@")
        self._menu_event_key_to_command = _normalize_menu_event_mapping(config.get("menu_event_key_to_command"))
        self._thread: threading.Thread | None = None
        self._main_loop: asyncio.AbstractEventLoop | None = None
        self._api_client = None
        self._CreateMessageReactionRequest = None
        self._CreateMessageReactionRequestBody = None
        self._Emoji = None
        self._PatchMessageRequest = None
        self._PatchMessageRequestBody = None
        self._background_tasks: set[asyncio.Task] = set()
        self._running_card_ids: dict[str, str] = {}
        self._running_card_tasks: dict[str, asyncio.Task] = {}
        self._message_render_modes: dict[str, str] = {}
        self._running_card_last_payload: dict[str, str] = {}
        self._user_last_chat_id: dict[str, str] = {}
        self._script_permission_guardrail_hits = 0
        self._contract_missing_for_chart_hits = 0
        self._CreateFileRequest = None
        self._CreateFileRequestBody = None
        self._CreateImageRequest = None
        self._CreateImageRequestBody = None
        self._GetMessageResourceRequest = None
        self._thread_lock = threading.Lock()
        logger.info(
            "[Feishu] config loaded: context_boundary=%s, reply_in_thread=%s, card_style=%s",
            self._context_boundary,
            self._reply_in_thread,
            self._card_style,
        )
        logger.info(
            "[Feishu] config loaded: render_mode=%s, require_mention_in_group=%s, menu_event_mappings=%d",
            self._render_mode,
            self._require_mention_in_group,
            len(self._menu_event_key_to_command),
        )
    @staticmethod
    def _parse_bool(raw: Any, *, default: bool) -> bool:
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            value = raw.strip().lower()
            if value in {"true", "1", "yes", "on"}:
                return True
            if value in {"false", "0", "no", "off"}:
                return False
        if raw is None:
            return default
        return bool(raw)

    def _normalize_context_boundary(self, raw: Any) -> str:
        value = str(raw).strip().lower() if raw is not None else "chat"
        if value not in _ALLOWED_CONTEXT_BOUNDARIES:
            logger.warning(
                "[Feishu] invalid context_boundary=%r, fallback to 'chat' (allowed=%s)",
                raw,
                sorted(_ALLOWED_CONTEXT_BOUNDARIES),
            )
            return "chat"
        return value

    def _normalize_card_style(self, raw: Any) -> str:
        value = str(raw).strip().lower() if raw is not None else "rich"
        if value not in _ALLOWED_CARD_STYLES:
            logger.warning(
                "[Feishu] invalid card_style=%r, fallback to 'rich' (allowed=%s)",
                raw,
                sorted(_ALLOWED_CARD_STYLES),
            )
            return "rich"
        return value

    def _normalize_render_mode(self, raw: Any) -> str:
        value = str(raw).strip().lower() if raw is not None else "auto"
        if value not in _ALLOWED_RENDER_MODES:
            logger.warning(
                "[Feishu] invalid render_mode=%r, fallback to 'auto' (allowed=%s)",
                raw,
                sorted(_ALLOWED_RENDER_MODES),
            )
            return "auto"
        return value

    def _resolve_topic_id(self, *, chat_id: str, msg_id: str, root_id: str | None) -> str:
        if self._context_boundary == "topic":
            return root_id or msg_id
        return chat_id

    @staticmethod
    def _is_group_message(message: Any) -> bool:
        chat_type = str(_safe_get(message, "chat_type", "") or "").strip().lower()
        return chat_type in _FEISHU_GROUP_CHAT_TYPES

    @staticmethod
    def _extract_at_mentions(content: Mapping[str, Any], message: Any) -> list[dict[str, Any]]:
        mentions: list[dict[str, Any]] = []

        def add_mention(raw: Any) -> None:
            if not isinstance(raw, Mapping):
                return
            user_id = _first_non_empty_str(
                raw.get("user_id"),
                raw.get("open_id"),
                raw.get("id"),
                _path_get(raw, "id.open_id"),
                _path_get(raw, "user_id.open_id"),
            )
            text = _first_non_empty_str(raw.get("text"), raw.get("name"), raw.get("user_name"), raw.get("tenant_key"))
            mentions.append({"user_id": user_id, "text": text})

        raw_mentions = _safe_get(message, "mentions", None)
        if isinstance(raw_mentions, list):
            for mention in raw_mentions:
                add_mention(mention)

        rich_content = content.get("content")
        if isinstance(rich_content, list):
            for paragraph in rich_content:
                if not isinstance(paragraph, list):
                    continue
                for element in paragraph:
                    if isinstance(element, Mapping) and element.get("tag") == "at":
                        add_mention(element)

        plain_text = content.get("text")
        if isinstance(plain_text, str):
            for match in _FEISHU_TEXT_MENTION_RE.finditer(plain_text):
                mentions.append({"user_id": "", "text": match.group(0), "placeholder": True})

        return mentions

    def _is_bot_mentioned(self, mentions: list[dict[str, Any]]) -> bool:
        if not mentions:
            return False

        if self._bot_open_id:
            if any(mention.get("user_id") == self._bot_open_id for mention in mentions):
                return True

        if self._bot_name:
            expected = self._bot_name.strip().lstrip("@")
            if any(mention.get("text", "").strip().lstrip("@") == expected for mention in mentions):
                return True

        # Some Feishu text events collapse mentions to placeholders like
        # @_user_1 and omit the target open_id/name. Keep this fallback narrow:
        # it only wakes the bot when Feishu provided an explicit mention marker.
        return any(mention.get("placeholder") is True for mention in mentions)

    def _strip_bot_mentions(self, text: str, mentions: list[dict[str, Any]]) -> str:
        cleaned = text
        for mention in mentions:
            mention_text = mention.get("text", "").strip()
            if not mention_text:
                continue
            if self._bot_open_id and mention.get("user_id") != self._bot_open_id:
                if mention.get("placeholder") is not True:
                    continue
            if self._bot_name and mention_text.lstrip("@") != self._bot_name and mention.get("placeholder") is not True:
                continue
            cleaned = cleaned.replace(mention_text, " ")
        return re.sub(r"[ \t]+", " ", cleaned).strip()

    def _is_visual_content(self, msg: OutboundMessage) -> bool:
        metadata = msg.metadata if isinstance(msg.metadata, dict) else {}
        if isinstance(metadata.get("feishu_skill_contract"), dict):
            return True
        payload = metadata.get("feishu_card_payload")
        if isinstance(payload, dict) and isinstance(payload.get("chart_spec"), dict):
            return True
        return False

    def _resolve_render_mode_for_message(self, msg: OutboundMessage) -> str:
        source_message_id = msg.thread_ts
        if source_message_id and source_message_id in self._message_render_modes:
            cached = self._message_render_modes[source_message_id]
            # Auto mode can start as text during streaming progress updates.
            # If a later chunk/final message carries explicit visual payload,
            # upgrade this source message to card rendering.
            if self._render_mode == "auto" and cached == "text" and self._is_visual_content(msg):
                self._message_render_modes[source_message_id] = "card"
                return "card"
            return cached

        if self._render_mode == "auto":
            resolved = "card" if self._is_visual_content(msg) else "text"
        else:
            resolved = self._render_mode

        if source_message_id:
            self._message_render_modes[source_message_id] = resolved
        return resolved

    @staticmethod
    def _build_markdown_table(columns: list[str], rows: list[list[Any]]) -> str:
        if not columns:
            return ""
        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"
        body = ["| " + " | ".join(str(cell) for cell in row[: len(columns)]) + " |" for row in rows]
        return "\n".join([header, separator, *body])

    @staticmethod
    def _build_table_card(columns: list[str], rows: list[list[str]], title: str = "Data Table") -> dict[str, Any]:
        markdown_table = FeishuChannel._build_markdown_table(columns, rows)
        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "header": {"title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "markdown", "content": markdown_table}],
        }

    @staticmethod
    def _validate_feishu_card_payload(payload: dict[str, Any]) -> None:
        title = payload.get("title")
        if title is not None and not isinstance(title, str):
            raise ValueError("feishu_card_payload.title must be a string")

        summary = payload.get("summary")
        if summary is not None and not isinstance(summary, str):
            raise ValueError("feishu_card_payload.summary must be a string")

        chart_spec = payload.get("chart_spec")
        if not isinstance(chart_spec, dict):
            raise ValueError("feishu_card_payload.chart_spec must be an object")

    def _build_card_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        elements: list[dict[str, Any]] = []
        title = payload.get("title")
        summary = payload.get("summary")
        if isinstance(title, str) and title.strip():
            elements.append({"tag": "markdown", "content": f"### {title.strip()}"})
        if isinstance(summary, str) and summary.strip():
            elements.append({"tag": "markdown", "content": summary.strip()})

        chart_spec = payload.get("chart_spec")
        if isinstance(chart_spec, dict):
            elements.append({"tag": "chart", "chart_spec": chart_spec})
        if not elements:
            elements.append({"tag": "markdown", "content": " "})

        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "elements": elements,
        }

    def _build_card_from_contract_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        elements: list[dict[str, Any]] = []
        title = payload.get("title")
        summary = payload.get("summary")
        if isinstance(title, str) and title.strip():
            elements.append({"tag": "markdown", "content": f"### {title.strip()}"})
        if isinstance(summary, str) and summary.strip():
            elements.append({"tag": "markdown", "content": summary.strip()})

        blocks = payload.get("blocks")
        if isinstance(blocks, list):
            for block in blocks:
                if not isinstance(block, dict):
                    continue
                block_type = str(block.get("type") or "").strip().lower()
                if block_type == "markdown":
                    markdown = block.get("markdown")
                    if isinstance(markdown, str) and markdown.strip():
                        elements.append(
                            {
                                "tag": "markdown",
                                "content": _sanitize_markdown_for_feishu_card(markdown.strip()),
                            }
                        )
                    continue

                if block_type == "table":
                    table = block.get("table")
                    if isinstance(table, dict):
                        columns = table.get("columns")
                        rows = table.get("rows")
                        if isinstance(columns, list) and isinstance(rows, list):
                            table_title = table.get("title")
                            table_card = self._build_table_card(
                                [str(col) for col in columns],
                                [[str(cell) for cell in row] for row in rows if isinstance(row, list)],
                                title=str(table_title) if isinstance(table_title, str) and table_title.strip() else "Data Table",
                            )
                            table_elements = table_card.get("elements")
                            if isinstance(table_elements, list):
                                for element in table_elements:
                                    if isinstance(element, dict):
                                        elements.append(element)
                    continue

                if block_type == "chart":
                    chart = block.get("chart")
                    if not isinstance(chart, dict):
                        continue
                    chart_type = str(chart.get("chart_type") or "").strip().lower()
                    dimension = chart.get("dimension")
                    metrics = chart.get("metrics")
                    if not isinstance(dimension, dict) or not isinstance(metrics, list):
                        continue
                    dim_name = str(dimension.get("name") or "x")
                    dim_values = dimension.get("values")
                    if not isinstance(dim_values, list):
                        continue

                    chart_spec: dict[str, Any] | None = None
                    if chart_type == "combo_bar_line":
                        normalized_series: list[dict[str, Any]] = []
                        for metric in metrics:
                            if not isinstance(metric, dict):
                                continue
                            metric_name = str(metric.get("name") or "value")
                            metric_values = metric.get("values")
                            if not isinstance(metric_values, list):
                                continue
                            series_type = str(metric.get("series_type") or "bar")
                            points = [
                                {"x": str(dim_values[idx]), "value": metric_values[idx]}
                                for idx in range(min(len(dim_values), len(metric_values)))
                            ]
                            normalized_series.append(
                                {
                                    "type": series_type,
                                    "name": metric_name,
                                    "data": {"values": points},
                                    "xField": "x",
                                    "yField": "value",
                                }
                            )
                        if normalized_series:
                            chart_spec = {
                                "type": "common",
                                "series": normalized_series,
                                "legend": {"visible": len(normalized_series) > 1},
                            }
                    else:
                        if chart_type in {"line", "bar"}:
                            values: list[dict[str, Any]] = []
                            for idx, x_val in enumerate(dim_values):
                                for metric in metrics:
                                    if not isinstance(metric, dict):
                                        continue
                                    metric_name = str(metric.get("name") or "value")
                                    metric_values = metric.get("values")
                                    if not isinstance(metric_values, list) or idx >= len(metric_values):
                                        continue
                                    values.append(
                                        {
                                            "dimension": str(x_val),
                                            "series": metric_name,
                                            "value": metric_values[idx],
                                        }
                                    )
                            if values:
                                chart_spec = {
                                    "type": chart_type,
                                    "data": {"values": values},
                                    "xField": "dimension",
                                    "yField": "value",
                                    "seriesField": "series",
                                }
                        elif chart_type == "pie":
                            metric = metrics[0] if metrics and isinstance(metrics[0], dict) else None
                            metric_values = metric.get("values") if isinstance(metric, dict) else None
                            if isinstance(metric_values, list):
                                values = [
                                    {"category": str(dim_values[idx]), "value": metric_values[idx]}
                                    for idx in range(min(len(dim_values), len(metric_values)))
                                ]
                                if values:
                                    chart_spec = {
                                        "type": "pie",
                                        "data": {"values": values},
                                        "categoryField": "category",
                                        "valueField": "value",
                                    }
                        elif chart_type == "scatter":
                            first = metrics[0] if len(metrics) > 0 and isinstance(metrics[0], dict) else None
                            second = metrics[1] if len(metrics) > 1 and isinstance(metrics[1], dict) else None
                            first_values = first.get("values") if isinstance(first, dict) else None
                            second_values = second.get("values") if isinstance(second, dict) else None
                            if isinstance(first_values, list) and isinstance(second_values, list):
                                count = min(len(dim_values), len(first_values), len(second_values))
                                values = [
                                    {
                                        "x": first_values[idx],
                                        "y": second_values[idx],
                                        "dimension": str(dim_values[idx]),
                                    }
                                    for idx in range(count)
                                ]
                                if values:
                                    chart_spec = {
                                        "type": "scatter",
                                        "data": {"values": values},
                                        "xField": "x",
                                        "yField": "y",
                                        "seriesField": "dimension",
                                    }

                    if isinstance(chart_spec, dict):
                        elements.append({"tag": "chart", "chart_spec": chart_spec})
                    continue

                if block_type == "image":
                    image = block.get("image")
                    if not isinstance(image, dict):
                        continue
                    caption = image.get("caption")
                    if isinstance(caption, str) and caption.strip():
                        elements.append({"tag": "markdown", "content": caption.strip()})
                    image_key = image.get("image_key")
                    if isinstance(image_key, str) and image_key.strip():
                        elements.append({"tag": "img", "img_key": image_key.strip()})

        if not elements:
            elements.append({"tag": "markdown", "content": " "})

        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "elements": elements,
        }

    def _resolve_card(self, msg: OutboundMessage) -> dict[str, Any]:
        metadata = msg.metadata if isinstance(msg.metadata, dict) else {}
        contract = metadata.get("feishu_skill_contract")
        if isinstance(contract, dict):
            logger.info("[Feishu] _resolve_card: using metadata.feishu_skill_contract (%s)", _contract_summary(contract))
            result = validate_and_normalize_contract(contract, channel_name=msg.channel_name)
            if result.ok and result.normalized:
                if result.normalized.get("render_mode") == "card":
                    payload = result.normalized.get("card_payload")
                    if isinstance(payload, dict):
                        logger.info("[Feishu] _resolve_card: contract validated -> render contract card")
                        return self._build_card_from_contract_payload(payload)
                fallback_text = str(result.normalized.get("fallback_text") or msg.text)
                logger.info("[Feishu] _resolve_card: contract validated -> fallback text card")
                return self._build_text_card(fallback_text)

            fallback_text = contract.get("fallback_text") if isinstance(contract.get("fallback_text"), str) else None
            logger.warning("[Feishu] _resolve_card: metadata contract invalid -> fallback text card")
            return self._build_text_card(str(fallback_text or msg.text))

        if isinstance(metadata.get("feishu_card_payload"), dict):
            payload = metadata["feishu_card_payload"]
            self._validate_feishu_card_payload(payload)
            logger.info("[Feishu] _resolve_card: using metadata.feishu_card_payload")
            return self._build_card_from_payload(payload)

        embedded_contract, embedded_payload = _extract_embedded_feishu_payloads(msg.text or "")
        if isinstance(embedded_contract, dict):
            logger.info("[Feishu] _resolve_card: extracted embedded feishu_skill_contract from text (%s)", _contract_summary(embedded_contract))
            result = validate_and_normalize_contract(embedded_contract, channel_name=msg.channel_name)
            if result.ok and result.normalized:
                if result.normalized.get("render_mode") == "card":
                    payload = result.normalized.get("card_payload")
                    if isinstance(payload, dict):
                        logger.info("[Feishu] _resolve_card: embedded contract validated -> render contract card")
                        return self._build_card_from_contract_payload(payload)
                fallback_text = str(result.normalized.get("fallback_text") or msg.text)
                logger.info("[Feishu] _resolve_card: embedded contract validated -> fallback text card")
                return self._build_text_card(fallback_text)
            logger.warning("[Feishu] _resolve_card: embedded contract invalid")

        if isinstance(embedded_payload, dict):
            try:
                self._validate_feishu_card_payload(embedded_payload)
                logger.info("[Feishu] _resolve_card: extracted embedded feishu_card_payload from text")
                return self._build_card_from_payload(embedded_payload)
            except Exception:
                logger.warning("[Feishu] embedded feishu_card_payload in text is invalid")

        logger.info("[Feishu] _resolve_card: no structured payload found -> render plain text card")
        return self._build_text_card(msg.text)

    def _build_text_card(self, text: str) -> dict[str, Any]:
        safe_text = _sanitize_markdown_for_feishu_card(text)
        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "elements": [{"tag": "markdown", "content": safe_text}],
        }

    @staticmethod
    def _build_progress_card(text: str, metadata: dict[str, Any]) -> dict[str, Any]:
        stage = metadata.get("status_stage")
        events = metadata.get("progress_events")
        timeline = metadata.get("progress_timeline")
        elapsed = metadata.get("progress_elapsed_seconds")
        lines: list[str] = []

        if isinstance(timeline, list) and timeline:
            for item in timeline[-80:]:
                if not isinstance(item, dict):
                    continue
                kind = str(item.get("kind") or "thought").strip().lower()
                kind_label = "行动" if kind == "action" else "思考"
                stage_label = str(item.get("stage") or "处理中")
                detail = str(item.get("detail") or "").strip()
                t = _format_elapsed_duration(item.get("elapsed_seconds"))
                if detail:
                    lines.append(f"[{t}] {kind_label} | {stage_label} | {detail}")
                else:
                    lines.append(f"[{t}] {kind_label} | {stage_label}")
        elif isinstance(events, list) and events:
            for item in events[-8:]:
                if not isinstance(item, dict):
                    continue
                stage_label = str(item.get("stage") or "处理中")
                detail = str(item.get("detail") or "").strip()
                if detail:
                    lines.append(f"{stage_label} | {detail}")
                else:
                    lines.append(stage_label)
        elif isinstance(stage, str) and stage.strip():
            lines.append(stage.strip())

        if text:
            lines.append(_sanitize_markdown_for_feishu_card(text))

        elapsed_seconds = max(0, int(elapsed)) if isinstance(elapsed, (int, float)) else 0
        if lines:
            lines[-1] = f"{lines[-1]} ({elapsed_seconds}s)"
        else:
            lines.append(f"({elapsed_seconds}s)")

        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "elements": [{"tag": "markdown", "content": "\n".join(lines)}],
        }

    @property
    def supports_streaming(self) -> bool:
        return True

    async def start(self) -> None:
        if self._running:
            return

        try:
            import lark_oapi as lark
            from lark_oapi.api.im.v1 import (
                CreateFileRequest,
                CreateFileRequestBody,
                CreateImageRequest,
                CreateImageRequestBody,
                CreateMessageReactionRequest,
                CreateMessageReactionRequestBody,
                CreateMessageRequest,
                CreateMessageRequestBody,
                Emoji,
                GetMessageResourceRequest,
                PatchMessageRequest,
                PatchMessageRequestBody,
                ReplyMessageRequest,
                ReplyMessageRequestBody,
            )
        except ImportError:
            logger.error("lark-oapi is not installed. Install it with: uv add lark-oapi")
            return

        self._lark = lark
        self._CreateMessageRequest = CreateMessageRequest
        self._CreateMessageRequestBody = CreateMessageRequestBody
        self._ReplyMessageRequest = ReplyMessageRequest
        self._ReplyMessageRequestBody = ReplyMessageRequestBody
        self._CreateMessageReactionRequest = CreateMessageReactionRequest
        self._CreateMessageReactionRequestBody = CreateMessageReactionRequestBody
        self._Emoji = Emoji
        self._PatchMessageRequest = PatchMessageRequest
        self._PatchMessageRequestBody = PatchMessageRequestBody
        self._CreateFileRequest = CreateFileRequest
        self._CreateFileRequestBody = CreateFileRequestBody
        self._CreateImageRequest = CreateImageRequest
        self._CreateImageRequestBody = CreateImageRequestBody
        self._GetMessageResourceRequest = GetMessageResourceRequest

        app_id = self.config.get("app_id", "")
        app_secret = self.config.get("app_secret", "")
        domain = self.config.get("domain", "https://open.feishu.cn")

        if not app_id or not app_secret:
            logger.error("Feishu channel requires app_id and app_secret")
            return

        self._api_client = lark.Client.builder().app_id(app_id).app_secret(app_secret).domain(domain).build()
        logger.info("[Feishu] using domain: %s", domain)
        self._main_loop = asyncio.get_event_loop()

        self._running = True
        self.bus.subscribe_outbound(self._on_outbound)

        # Both ws.Client construction and start() must happen in a dedicated
        # thread with its own event loop.  lark-oapi caches the running loop
        # at construction time and later calls loop.run_until_complete(),
        # which conflicts with an already-running uvloop.
        self._thread = threading.Thread(
            target=self._run_ws,
            args=(app_id, app_secret, domain),
            daemon=True,
        )
        self._thread.start()
        logger.info("Feishu channel started")

    def _run_ws(self, app_id: str, app_secret: str, domain: str) -> None:
        """Construct and run the lark WS client in a thread with a fresh event loop.

        The lark-oapi SDK captures a module-level event loop at import time
        (``lark_oapi.ws.client.loop``).  When uvicorn uses uvloop, that
        captured loop is the *main* thread's uvloop - which is already
        running, so ``loop.run_until_complete()`` inside ``Client.start()``
        raises ``RuntimeError``.

        We work around this by creating a plain asyncio event loop for this
        thread and patching the SDK's module-level reference before calling
        ``start()``.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            import lark_oapi as lark
            import lark_oapi.ws.client as _ws_client_mod

            # Replace the SDK's module-level loop so Client.start() uses
            # this thread's (non-running) event loop instead of the main
            # thread's uvloop.
            _ws_client_mod.loop = loop

            builder = lark.EventDispatcherHandler.builder("", "").register_p2_im_message_receive_v1(self._on_message)
            menu_register_candidates = [
                "register_p2_application_bot_menu_v6",
                "register_p2_application_bot_menu_v4",
                "register_p1_application_bot_menu_v6",
                "register_p1_application_bot_menu_v4",
            ]
            registered_menu = False
            for method_name in menu_register_candidates:
                register_fn = getattr(builder, method_name, None)
                if callable(register_fn):
                    builder = register_fn(self._on_menu_event)
                    registered_menu = True
                    logger.info("[Feishu] registered menu event handler via %s", method_name)
                    break
            if not registered_menu:
                logger.warning("[Feishu] bot-menu event registration not available in current lark-oapi SDK")
            event_handler = builder.build()
            ws_client = lark.ws.Client(
                app_id=app_id,
                app_secret=app_secret,
                event_handler=event_handler,
                log_level=lark.LogLevel.INFO,
                domain=domain,
            )
            ws_client.start()
        except Exception:
            if self._running:
                logger.exception("Feishu WebSocket error")

    async def stop(self) -> None:
        self._running = False
        self.bus.unsubscribe_outbound(self._on_outbound)
        for task in list(self._background_tasks):
            task.cancel()
        self._background_tasks.clear()
        for task in list(self._running_card_tasks.values()):
            task.cancel()
        self._running_card_tasks.clear()
        self._message_render_modes.clear()
        self._running_card_last_payload.clear()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("Feishu channel stopped")

    def get_runtime_metrics(self) -> dict[str, Any]:
        return {
            "script_permission_guardrail_hits": self._script_permission_guardrail_hits,
            "contract_missing_for_chart_hits": self._contract_missing_for_chart_hits,
        }

    async def send(self, msg: OutboundMessage, *, _max_retries: int = 3) -> None:
        if not self._api_client:
            logger.warning("[Feishu] send called but no api_client available")
            return

        guarded_text = _apply_script_permission_guardrail(msg.text)
        if guarded_text != msg.text:
            self._script_permission_guardrail_hits += 1
            logger.warning(
                "[Feishu] script-permission guardrail hit: count=%d chat_id=%s source=%s final=%s",
                self._script_permission_guardrail_hits,
                msg.chat_id,
                msg.thread_ts,
                msg.is_final,
            )
        msg.text = guarded_text
        if isinstance(msg.metadata, dict) and msg.metadata.get("feishu_contract_missing_for_chart"):
            self._contract_missing_for_chart_hits += 1
            logger.warning(
                "[Feishu] chart contract missing for chart-intent request: count=%d chat_id=%s source=%s",
                self._contract_missing_for_chart_hits,
                msg.chat_id,
                msg.thread_ts,
            )

        logger.info(
            "[Feishu] sending reply: chat_id=%s, thread_ts=%s, text_len=%d",
            msg.chat_id,
            msg.thread_ts,
            len(msg.text),
        )
        mode = self._resolve_render_mode_for_message(msg)
        logger.info(
            "[Feishu] resolved render mode: mode=%s, final=%s, chat_id=%s, source=%s",
            mode,
            msg.is_final,
            msg.chat_id,
            msg.thread_ts,
        )

        if mode == "text" and not msg.is_final:
            logger.debug("[Feishu] skip non-final text update for source=%s", msg.thread_ts)
            return

        last_exc: Exception | None = None
        for attempt in range(_max_retries):
            try:
                if mode == "text":
                    await self._send_text_message(msg)
                else:
                    await self._send_card_message(msg)
                if msg.is_final and msg.thread_ts:
                    self._message_render_modes.pop(msg.thread_ts, None)
                return  # success
            except Exception as exc:
                last_exc = exc
                if attempt < _max_retries - 1:
                    delay = 2**attempt  # 1s, 2s
                    logger.warning(
                        "[Feishu] send failed (attempt %d/%d), retrying in %ds: %s",
                        attempt + 1,
                        _max_retries,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)

        logger.error("[Feishu] send failed after %d attempts: %s", _max_retries, last_exc)
        if mode == "card" and msg.is_final:
            has_existing_card = bool(msg.thread_ts and self._running_card_ids.get(msg.thread_ts))
            if has_existing_card:
                # A card already exists for this source message; avoid sending a
                # second text reply that creates duplicate final outputs.
                logger.warning(
                    "[Feishu] card delivery failed after retries and an existing card is present; skip text fallback to avoid duplicate replies: chat_id=%s source=%s",
                    msg.chat_id,
                    msg.thread_ts,
                )
            else:
                logger.warning(
                    "[Feishu] card delivery failed after retries, fallback to text reply: chat_id=%s source=%s",
                    msg.chat_id,
                    msg.thread_ts,
                )
                try:
                    await self._send_text_message(msg)
                    if msg.thread_ts:
                        self._message_render_modes.pop(msg.thread_ts, None)
                    return
                except Exception as fallback_exc:
                    logger.exception("[Feishu] text fallback also failed for source=%s", msg.thread_ts)
                    last_exc = fallback_exc
        if msg.is_final and msg.thread_ts:
            self._message_render_modes.pop(msg.thread_ts, None)
        if last_exc is None:
            raise RuntimeError("Feishu send failed without an exception from any attempt")
        raise last_exc

    async def send_file(self, msg: OutboundMessage, attachment: ResolvedAttachment) -> bool:
        if not self._api_client:
            return False

        # Check size limits (image: 10MB, file: 30MB)
        if attachment.is_image and attachment.size > 10 * 1024 * 1024:
            logger.warning("[Feishu] image too large (%d bytes), skipping: %s", attachment.size, attachment.filename)
            return False
        if not attachment.is_image and attachment.size > 30 * 1024 * 1024:
            logger.warning("[Feishu] file too large (%d bytes), skipping: %s", attachment.size, attachment.filename)
            return False

        try:
            if attachment.is_image:
                file_key = await self._upload_image(attachment.actual_path)
                msg_type = "image"
                content = json.dumps({"image_key": file_key})
            else:
                file_key = await self._upload_file(attachment.actual_path, attachment.filename)
                msg_type = "file"
                content = json.dumps({"file_key": file_key})

            if msg.thread_ts:
                request = (
                    self._ReplyMessageRequest.builder()
                    .message_id(msg.thread_ts)
                    .request_body(
                        self._ReplyMessageRequestBody.builder()
                        .msg_type(msg_type)
                        .content(content)
                        .reply_in_thread(self._reply_in_thread)
                        .build()
                    )
                    .build()
                )
                await asyncio.to_thread(self._api_client.im.v1.message.reply, request)
            else:
                receive_id_type = _resolve_receive_id_type(msg.chat_id)
                request = (
                    self._CreateMessageRequest.builder()
                    .receive_id_type(receive_id_type)
                    .request_body(self._CreateMessageRequestBody.builder().receive_id(msg.chat_id).msg_type(msg_type).content(content).build())
                    .build()
                )
                await asyncio.to_thread(self._api_client.im.v1.message.create, request)

            logger.info("[Feishu] file sent: %s (type=%s)", attachment.filename, msg_type)
            return True
        except Exception:
            logger.exception("[Feishu] failed to upload/send file: %s", attachment.filename)
            return False

    async def _upload_image(self, path) -> str:
        """Upload an image to Feishu and return the image_key."""
        with open(str(path), "rb") as f:
            request = self._CreateImageRequest.builder().request_body(self._CreateImageRequestBody.builder().image_type("message").image(f).build()).build()
            response = await asyncio.to_thread(self._api_client.im.v1.image.create, request)
        if not response.success():
            raise RuntimeError(f"Feishu image upload failed: code={response.code}, msg={response.msg}")
        return response.data.image_key

    async def _upload_file(self, path, filename: str) -> str:
        """Upload a file to Feishu and return the file_key."""
        suffix = path.suffix.lower() if hasattr(path, "suffix") else ""
        if suffix in (".xls", ".xlsx", ".csv"):
            file_type = "xls"
        elif suffix in (".ppt", ".pptx"):
            file_type = "ppt"
        elif suffix == ".pdf":
            file_type = "pdf"
        elif suffix in (".doc", ".docx"):
            file_type = "doc"
        else:
            file_type = "stream"

        with open(str(path), "rb") as f:
            request = self._CreateFileRequest.builder().request_body(self._CreateFileRequestBody.builder().file_type(file_type).file_name(filename).file(f).build()).build()
            response = await asyncio.to_thread(self._api_client.im.v1.file.create, request)
        if not response.success():
            raise RuntimeError(f"Feishu file upload failed: code={response.code}, msg={response.msg}")
        return response.data.file_key

    async def receive_file(self, msg: InboundMessage, thread_id: str) -> InboundMessage:
        """Download a Feishu file into the thread uploads directory.

        Returns the sandbox virtual path when the image is persisted successfully.
        """
        if not msg.thread_ts:
            logger.warning("[Feishu] received file message without thread_ts, cannot associate with conversation: %s", msg)
            return msg
        files = msg.files
        if not files:
            logger.warning("[Feishu] received message with no files: %s", msg)
            return msg
        text = msg.text
        for file in files:
            if file.get("image_key"):
                virtual_path = await self._receive_single_file(msg.thread_ts, file["image_key"], "image", thread_id)
                text = text.replace("[image]", virtual_path, 1)
            elif file.get("file_key"):
                virtual_path = await self._receive_single_file(msg.thread_ts, file["file_key"], "file", thread_id)
                text = text.replace("[file]", virtual_path, 1)
        msg.text = text
        return msg

    async def _receive_single_file(self, message_id: str, file_key: str, type: Literal["image", "file"], thread_id: str) -> str:
        request = self._GetMessageResourceRequest.builder().message_id(message_id).file_key(file_key).type(type).build()

        def inner():
            return self._api_client.im.v1.message_resource.get(request)

        try:
            response = await asyncio.to_thread(inner)
        except Exception:
            logger.exception("[Feishu] resource get request failed for resource_key=%s type=%s", file_key, type)
            return f"Failed to obtain the [{type}]"

        if not response.success():
            logger.warning(
                "[Feishu] resource get failed: resource_key=%s, type=%s, code=%s, msg=%s, log_id=%s ",
                file_key,
                type,
                response.code,
                response.msg,
                response.get_log_id(),
            )
            return f"Failed to obtain the [{type}]"

        image_stream = getattr(response, "file", None)
        if image_stream is None:
            logger.warning("[Feishu] resource get returned no file stream: resource_key=%s, type=%s", file_key, type)
            return f"Failed to obtain the [{type}]"

        try:
            content: bytes = await asyncio.to_thread(image_stream.read)
        except Exception:
            logger.exception("[Feishu] failed to read resource stream: resource_key=%s, type=%s", file_key, type)
            return f"Failed to obtain the [{type}]"

        if not content:
            logger.warning("[Feishu] empty resource content: resource_key=%s, type=%s", file_key, type)
            return f"Failed to obtain the [{type}]"

        paths = get_paths()
        user_id = get_effective_user_id()
        paths.ensure_thread_dirs(thread_id, user_id=user_id)
        uploads_dir = paths.sandbox_uploads_dir(thread_id, user_id=user_id).resolve()

        ext = "png" if type == "image" else "bin"
        raw_filename = getattr(response, "file_name", "") or f"feishu_{file_key[-12:]}.{ext}"

        # Sanitize filename: preserve extension, replace path chars in name part
        if "." in raw_filename:
            name_part, ext = raw_filename.rsplit(".", 1)
            name_part = re.sub(r"[./\\]", "_", name_part)
            filename = f"{name_part}.{ext}"
        else:
            filename = re.sub(r"[./\\]", "_", raw_filename)
        resolved_target = uploads_dir / filename

        def down_load():
            # use thread_lock to avoid filename conflicts when writing
            with self._thread_lock:
                resolved_target.write_bytes(content)

        try:
            await asyncio.to_thread(down_load)
        except Exception:
            logger.exception("[Feishu] failed to persist downloaded resource: %s, type=%s", resolved_target, type)
            return f"Failed to obtain the [{type}]"

        virtual_path = f"{VIRTUAL_PATH_PREFIX}/uploads/{resolved_target.name}"

        try:
            sandbox_provider = get_sandbox_provider()
            sandbox_id = sandbox_provider.acquire(thread_id)
            if sandbox_id != "local":
                sandbox = sandbox_provider.get(sandbox_id)
                if sandbox is None:
                    logger.warning("[Feishu] sandbox not found for thread_id=%s", thread_id)
                    return f"Failed to obtain the [{type}]"
                sandbox.update_file(virtual_path, content)
        except Exception:
            logger.exception("[Feishu] failed to sync resource into non-local sandbox: %s", virtual_path)
            return f"Failed to obtain the [{type}]"

        logger.info("[Feishu] downloaded resource mapped: file_key=%s -> %s", file_key, virtual_path)
        return virtual_path

    # -- message formatting ------------------------------------------------

    @staticmethod
    def _build_card_content(card: dict[str, Any]) -> str:
        return json.dumps(card)

    # -- reaction helpers --------------------------------------------------

    async def _add_reaction(self, message_id: str, emoji_type: str = "THUMBSUP") -> None:
        """Add an emoji reaction to a message."""
        if not self._api_client or not self._CreateMessageReactionRequest:
            return
        try:
            request = self._CreateMessageReactionRequest.builder().message_id(message_id).request_body(self._CreateMessageReactionRequestBody.builder().reaction_type(self._Emoji.builder().emoji_type(emoji_type).build()).build()).build()
            await asyncio.to_thread(self._api_client.im.v1.message_reaction.create, request)
            logger.info("[Feishu] reaction '%s' added to message %s", emoji_type, message_id)
        except Exception:
            logger.exception("[Feishu] failed to add reaction '%s' to message %s", emoji_type, message_id)

    @staticmethod
    def _ensure_api_success(response, action: str) -> None:
        if response is None or not response.success():
            code = getattr(response, "code", None)
            msg = getattr(response, "msg", None)
            raise RuntimeError(f"Feishu {action} failed: code={code}, msg={msg}")

    async def _reply_text(self, message_id: str, text: str) -> str | None:
        if not self._api_client:
            return None
        request = (
            self._ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                self._ReplyMessageRequestBody.builder()
                .msg_type("text")
                .content(json.dumps({"text": text}))
                .reply_in_thread(self._reply_in_thread)
                .build()
            )
            .build()
        )
        response = await asyncio.to_thread(self._api_client.im.v1.message.reply, request)
        self._ensure_api_success(response, "reply_text")
        response_data = getattr(response, "data", None)
        return getattr(response_data, "message_id", None)

    async def _create_text(self, chat_id: str, text: str) -> None:
        if not self._api_client:
            return
        receive_id_type = _resolve_receive_id_type(chat_id)
        request = (
            self._CreateMessageRequest.builder()
            .receive_id_type(receive_id_type)
            .request_body(
                self._CreateMessageRequestBody.builder()
                .receive_id(chat_id)
                .msg_type("text")
                .content(json.dumps({"text": text}))
                .build()
            )
            .build()
        )
        response = await asyncio.to_thread(self._api_client.im.v1.message.create, request)
        self._ensure_api_success(response, "create_text")

    async def _reply_card(self, message_id: str, card: dict[str, Any]) -> str | None:
        """Reply with an interactive card and return the created card message ID."""
        if not self._api_client:
            return None

        content = self._build_card_content(card)
        request = (
            self._ReplyMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                self._ReplyMessageRequestBody.builder()
                .msg_type("interactive")
                .content(content)
                .reply_in_thread(self._reply_in_thread)
                .build()
            )
            .build()
        )
        response = await asyncio.to_thread(self._api_client.im.v1.message.reply, request)
        self._ensure_api_success(response, "reply_card")
        response_data = getattr(response, "data", None)
        return getattr(response_data, "message_id", None)

    async def _create_card(self, chat_id: str, card: dict[str, Any]) -> None:
        """Create a new card message in the target chat."""
        if not self._api_client:
            return

        content = self._build_card_content(card)
        receive_id_type = _resolve_receive_id_type(chat_id)
        request = (
            self._CreateMessageRequest.builder()
            .receive_id_type(receive_id_type)
            .request_body(self._CreateMessageRequestBody.builder().receive_id(chat_id).msg_type("interactive").content(content).build())
            .build()
        )
        response = await asyncio.to_thread(self._api_client.im.v1.message.create, request)
        self._ensure_api_success(response, "create_card")

    async def _update_card(self, message_id: str, card: dict[str, Any]) -> None:
        """Patch an existing card message in place."""
        if not self._api_client or not self._PatchMessageRequest:
            return

        content = self._build_card_content(card)
        request = self._PatchMessageRequest.builder().message_id(message_id).request_body(self._PatchMessageRequestBody.builder().content(content).build()).build()
        response = await asyncio.to_thread(self._api_client.im.v1.message.patch, request)
        self._ensure_api_success(response, "update_card")

    def _track_background_task(self, task: asyncio.Task, *, name: str, msg_id: str) -> None:
        """Keep a strong reference to fire-and-forget tasks and surface errors."""
        self._background_tasks.add(task)
        task.add_done_callback(lambda done_task, task_name=name, mid=msg_id: self._finalize_background_task(done_task, task_name, mid))

    def _finalize_background_task(self, task: asyncio.Task, name: str, msg_id: str) -> None:
        self._background_tasks.discard(task)
        self._log_task_error(task, name, msg_id)

    async def _create_running_card(self, source_message_id: str, text: str) -> str | None:
        """Create the running card and cache its message ID when available."""
        running_card = self._build_progress_card(text, {"status_stage": "已接收请求", "progress_events": []})
        running_card_id = await self._reply_card(source_message_id, running_card)
        if running_card_id:
            self._running_card_ids[source_message_id] = running_card_id
            logger.info("[Feishu] running card created: source=%s card=%s", source_message_id, running_card_id)
        else:
            logger.warning("[Feishu] running card creation returned no message_id for source=%s, subsequent updates will fall back to new replies", source_message_id)
        return running_card_id

    def _ensure_running_card_started(self, source_message_id: str, text: str = "处理中...") -> asyncio.Task | None:
        """Start running-card creation once per source message."""
        running_card_id = self._running_card_ids.get(source_message_id)
        if running_card_id:
            return None

        running_card_task = self._running_card_tasks.get(source_message_id)
        if running_card_task:
            return running_card_task

        running_card_task = asyncio.create_task(self._create_running_card(source_message_id, text))
        self._running_card_tasks[source_message_id] = running_card_task
        running_card_task.add_done_callback(lambda done_task, mid=source_message_id: self._finalize_running_card_task(mid, done_task))
        return running_card_task

    def _finalize_running_card_task(self, source_message_id: str, task: asyncio.Task) -> None:
        if self._running_card_tasks.get(source_message_id) is task:
            self._running_card_tasks.pop(source_message_id, None)
        self._log_task_error(task, "create_running_card", source_message_id)

    async def _ensure_running_card(self, source_message_id: str, text: str = "处理中...") -> str | None:
        """Ensure the in-thread running card exists and track its message ID."""
        running_card_id = self._running_card_ids.get(source_message_id)
        if running_card_id:
            return running_card_id

        running_card_task = self._ensure_running_card_started(source_message_id, text)
        if running_card_task is None:
            return self._running_card_ids.get(source_message_id)
        return await running_card_task

    async def _send_running_reply(self, message_id: str) -> None:
        """Reply to a message in-thread with a running card."""
        try:
            await self._ensure_running_card(message_id)
        except Exception:
            logger.exception("[Feishu] failed to send running reply for message %s", message_id)

    async def _send_text_message(self, msg: OutboundMessage) -> None:
        source_message_id = msg.thread_ts
        if source_message_id:
            await self._reply_text(source_message_id, msg.text)
            if msg.is_final:
                await self._add_reaction(source_message_id, "DONE")
            return
        await self._create_text(msg.chat_id, msg.text)

    async def _send_card_message(self, msg: OutboundMessage) -> None:
        """Send or update the Feishu card tied to the current request."""
        source_message_id = msg.thread_ts
        card = self._resolve_card(msg)
        if source_message_id:
            running_card_id = self._running_card_ids.get(source_message_id)
            awaited_running_card_task = False

            if not running_card_id:
                running_card_task = self._running_card_tasks.get(source_message_id)
                if running_card_task:
                    awaited_running_card_task = True
                    running_card_id = await running_card_task

            if running_card_id:
                try:
                    if not msg.is_final and isinstance(msg.metadata, dict):
                        card = self._build_progress_card(msg.text, msg.metadata)
                    payload_str = json.dumps(card, ensure_ascii=False, sort_keys=True)
                    if not msg.is_final and self._running_card_last_payload.get(source_message_id) == payload_str:
                        return
                    await self._update_card(running_card_id, card)
                    if not msg.is_final:
                        self._running_card_last_payload[source_message_id] = payload_str
                except Exception:
                    if not msg.is_final:
                        raise
                    logger.exception(
                        "[Feishu] failed to patch running card %s, falling back to final reply",
                        running_card_id,
                    )
                    await self._reply_card(source_message_id, card)
                else:
                    logger.info("[Feishu] running card updated: source=%s card=%s", source_message_id, running_card_id)
            elif msg.is_final:
                await self._reply_card(source_message_id, card)
            elif awaited_running_card_task:
                logger.warning(
                    "[Feishu] running card task finished without message_id for source=%s, skipping duplicate non-final creation",
                    source_message_id,
                )
            else:
                await self._ensure_running_card(source_message_id, msg.text)

            if msg.is_final:
                self._running_card_ids.pop(source_message_id, None)
                self._running_card_last_payload.pop(source_message_id, None)
                await self._add_reaction(source_message_id, "DONE")
            return

        await self._create_card(msg.chat_id, card)

    # -- internal ----------------------------------------------------------

    @staticmethod
    def _log_future_error(fut, name: str, msg_id: str) -> None:
        """Callback for run_coroutine_threadsafe futures to surface errors."""
        try:
            exc = fut.exception()
            if exc:
                logger.error("[Feishu] %s failed for msg_id=%s: %s", name, msg_id, exc)
        except Exception:
            pass

    @staticmethod
    def _log_task_error(task: asyncio.Task, name: str, msg_id: str) -> None:
        """Callback for background asyncio tasks to surface errors."""
        try:
            exc = task.exception()
            if exc:
                logger.error("[Feishu] %s failed for msg_id=%s: %s", name, msg_id, exc)
        except asyncio.CancelledError:
            logger.info("[Feishu] %s cancelled for msg_id=%s", name, msg_id)
        except Exception:
            pass

    async def _prepare_inbound(self, msg_id: str, inbound) -> None:
        """Kick off Feishu side effects without delaying inbound dispatch."""
        reaction_task = asyncio.create_task(self._add_reaction(msg_id, "OK"))
        self._track_background_task(reaction_task, name="add_reaction", msg_id=msg_id)
        if self._render_mode == "card":
            self._ensure_running_card_started(msg_id)
        await self.bus.publish_inbound(inbound)

    @staticmethod
    def _extract_menu_event_payload(event: Any) -> tuple[str | None, str | None, str | None, str | None]:
        """Best-effort extraction for Feishu bot-menu push event payload."""
        event_data = _safe_get(event, "event")
        header = _safe_get(event, "header")
        event_type = str(_safe_get(header, "event_type", "") or "")
        if event_data is None:
            return None, None, None, event_type or None

        event_key = _first_non_empty_str(
            _path_get(event_data, "event_key"),
            _path_get(event_data, "key"),
            _path_get(event_data, "action.value"),
            _path_get(event_data, "action.name"),
            _path_get(event_data, "option"),
            _path_get(event, "event_key"),
            _path_get(event, "key"),
            _path_get(event, "action.value"),
            _path_get(event, "action.name"),
            event_type,
        )
        user_id = _first_non_empty_str(
            _path_get(event_data, "operator.operator_id.open_id"),
            _path_get(event_data, "operator.open_id"),
            _path_get(event_data, "user_id.open_id"),
            _path_get(event_data, "user.open_id"),
            _path_get(event, "operator.operator_id.open_id"),
            _path_get(event, "operator.open_id"),
            _path_get(event, "user.open_id"),
        )

        open_chat_id = _first_non_empty_str(
            _path_get(event_data, "context.open_chat_id"),
            _path_get(event_data, "context.chat_id"),
            _path_get(event_data, "open_chat_id"),
            _path_get(event_data, "chat_id"),
            _path_get(event_data, "message.chat_id"),
            _path_get(event_data, "action.context.open_chat_id"),
            _path_get(event_data, "action.context.chat_id"),
            _path_get(event, "event.context.open_chat_id"),
            _path_get(event, "event.context.chat_id"),
            _path_get(event, "event.open_chat_id"),
            _path_get(event, "event.chat_id"),
            _path_get(event, "event.message.chat_id"),
        )
        open_message_id = _first_non_empty_str(
            _path_get(event_data, "context.open_message_id"),
            _path_get(event_data, "context.message_id"),
            _path_get(event_data, "open_message_id"),
            _path_get(event_data, "message_id"),
            _path_get(event_data, "message.message_id"),
            _path_get(event_data, "action.context.open_message_id"),
            _path_get(event_data, "action.context.message_id"),
            _path_get(event, "event.context.open_message_id"),
            _path_get(event, "event.context.message_id"),
            _path_get(event, "event.open_message_id"),
            _path_get(event, "event.message_id"),
            _path_get(event, "event.message.message_id"),
        )
        return open_chat_id or None, open_message_id or None, user_id or None, event_key or event_type or None

    def _on_menu_event(self, event) -> None:
        """Called by lark-oapi when a bot custom-menu event is pushed."""
        try:
            chat_id, message_id, user_id, event_key = self._extract_menu_event_payload(event)
            # Keep delivery target and conversation key separate:
            # - delivery target can be open_id (for immediate feedback),
            # - conversation key must remain chat/topic scoped to avoid cross-chat session/thread pollution.
            reply_target_id = chat_id or user_id

            if not chat_id and user_id:
                fallback_chat = self._user_last_chat_id.get(user_id)
                if fallback_chat:
                    chat_id = fallback_chat
                    logger.info(
                        "[Feishu] menu event resolved chat_id from user cache: user_id=%s chat_id=%s event_key=%s",
                        user_id,
                        chat_id,
                        event_key,
                    )
                else:
                    logger.info("[Feishu] menu event has no chat_id and no cache hit: user_id=%s event_key=%s", user_id, event_key)
            if not chat_id or not event_key:
                logger.warning(
                    "[Feishu] menu event missing chat_id/event_key, ignored: parsed_chat_id=%r parsed_event_key=%r payload=%s",
                    chat_id,
                    event_key,
                    json.dumps(_sanitize_for_log(event), ensure_ascii=False, default=str),
                )
                # If we only have user identity (open_id), send a direct hint instead of silently dropping.
                # Do NOT route command with open_id as conversation key; that would break per-chat thread/session semantics.
                if event_key and reply_target_id and self._main_loop and self._main_loop.is_running():
                    hint = (
                        f"已收到菜单操作：{event_key}\n"
                        "当前事件缺少会话ID，无法绑定到群会话上下文。"
                        "请先在目标会话发送一条消息后再点击菜单。"
                    )
                    reply = OutboundMessage(
                        channel_name="feishu",
                        chat_id=reply_target_id,
                        thread_id="",
                        text=hint,
                        thread_ts=message_id,
                    )
                    fut = asyncio.run_coroutine_threadsafe(self.bus.publish_outbound(reply), self._main_loop)
                    fut.add_done_callback(lambda f, mid=message_id or f"menu:{reply_target_id}": self._log_future_error(f, "publish_menu_hint", mid))
                return

            mapped_command = self._menu_event_key_to_command.get(event_key.strip().lower())
            if not mapped_command:
                logger.warning("[Feishu] unknown menu event key: %s", event_key)
                if self._main_loop and self._main_loop.is_running():
                    reply = OutboundMessage(
                        channel_name="feishu",
                        chat_id=reply_target_id or chat_id,
                        thread_id="",
                        text=f"Unsupported menu event: {event_key}",
                        thread_ts=message_id,
                    )
                    target = reply_target_id or chat_id or "menu"
                    fut = asyncio.run_coroutine_threadsafe(self.bus.publish_outbound(reply), self._main_loop)
                    fut.add_done_callback(lambda f, mid=message_id or f"menu:{target}": self._log_future_error(f, "publish_menu_unknown", mid))
                return

            inbound = self._make_inbound(
                chat_id=chat_id,
                user_id=user_id or "",
                text=mapped_command,
                msg_type=InboundMessageType.COMMAND,
                thread_ts=message_id,
                metadata={
                    "source": "feishu_menu",
                    "event_key": event_key,
                    "render_mode": self._render_mode,
                },
            )
            inbound.topic_id = message_id or chat_id
            if self._main_loop and self._main_loop.is_running():
                source_id = message_id or f"menu:{chat_id}"
                fut = asyncio.run_coroutine_threadsafe(self._prepare_inbound(source_id, inbound), self._main_loop)
                fut.add_done_callback(lambda f, mid=source_id: self._log_future_error(f, "prepare_inbound(menu)", mid))
            else:
                logger.warning("[Feishu] main loop not running, cannot publish menu event")
        except Exception:
            logger.exception("[Feishu] error processing menu event")

    def _on_message(self, event) -> None:
        """Called by lark-oapi when a message is received (runs in lark thread)."""
        try:
            logger.info("[Feishu] raw event received: type=%s", type(event).__name__)
            message = event.event.message
            chat_id = message.chat_id
            msg_id = message.message_id
            sender_id = event.event.sender.sender_id.open_id
            if isinstance(sender_id, str) and sender_id and isinstance(chat_id, str) and chat_id:
                self._user_last_chat_id[sender_id] = chat_id

            # root_id is set when the message is a reply within a Feishu thread.
            # Use it as topic_id so all replies share the same DeerFlow thread.
            root_id = getattr(message, "root_id", None) or None

            # Parse message content
            content = json.loads(message.content)
            is_group_message = self._is_group_message(message)
            mentions = self._extract_at_mentions(content, message)
            bot_mentioned = self._is_bot_mentioned(mentions)

            # files_list store the any-file-key in feishu messages, which can be used to download the file content later
            # In Feishu channel, image_keys are independent of file_keys.
            # The file_key includes files, videos, and audio, but does not include stickers.
            files_list = []

            if "text" in content:
                # Handle plain text messages
                text = content["text"]
            elif "file_key" in content:
                file_key = content.get("file_key")
                if isinstance(file_key, str) and file_key:
                    files_list.append({"file_key": file_key})
                    text = "[file]"
                else:
                    text = ""
            elif "image_key" in content:
                image_key = content.get("image_key")
                if isinstance(image_key, str) and image_key:
                    files_list.append({"image_key": image_key})
                    text = "[image]"
                else:
                    text = ""
            elif "content" in content and isinstance(content["content"], list):
                # Handle rich-text messages with a top-level "content" list (e.g., topic groups/posts)
                text_paragraphs: list[str] = []
                for paragraph in content["content"]:
                    if isinstance(paragraph, list):
                        paragraph_text_parts: list[str] = []
                        for element in paragraph:
                            if isinstance(element, dict):
                                # Include both normal text and @ mentions
                                if element.get("tag") in ("text", "at"):
                                    text_value = element.get("text", "")
                                    if text_value:
                                        paragraph_text_parts.append(text_value)
                                elif element.get("tag") == "img":
                                    image_key = element.get("image_key")
                                    if isinstance(image_key, str) and image_key:
                                        files_list.append({"image_key": image_key})
                                        paragraph_text_parts.append("[image]")
                                elif element.get("tag") in ("file", "media"):
                                    file_key = element.get("file_key")
                                    if isinstance(file_key, str) and file_key:
                                        files_list.append({"file_key": file_key})
                                        paragraph_text_parts.append("[file]")
                        if paragraph_text_parts:
                            # Join text segments within a paragraph with spaces to avoid "helloworld"
                            text_paragraphs.append(" ".join(paragraph_text_parts))

                # Join paragraphs with blank lines to preserve paragraph boundaries
                text = "\n\n".join(text_paragraphs)
            else:
                text = ""
            text = text.strip()

            logger.info(
                "[Feishu] parsed message: chat_id=%s, msg_id=%s, root_id=%s, sender=%s, group=%s, mentioned=%s, text=%r",
                chat_id,
                msg_id,
                root_id,
                sender_id,
                is_group_message,
                bot_mentioned,
                text[:100] if text else "",
            )

            if not (text or files_list):
                logger.info("[Feishu] empty text, ignoring message")
                return

            # Only treat known slash commands as commands; absolute paths and
            # other slash-prefixed text should be handled as normal chat.
            if _is_feishu_command(text):
                msg_type = InboundMessageType.COMMAND
            else:
                msg_type = InboundMessageType.CHAT

            if self._require_mention_in_group and is_group_message and not bot_mentioned and msg_type != InboundMessageType.COMMAND:
                logger.info("[Feishu] group message without bot mention, ignoring: chat_id=%s, msg_id=%s", chat_id, msg_id)
                return

            if is_group_message and bot_mentioned:
                text = self._strip_bot_mentions(text, mentions)
                if not (text or files_list):
                    logger.info("[Feishu] group message only mentioned bot, ignoring: chat_id=%s, msg_id=%s", chat_id, msg_id)
                    return

            topic_id = self._resolve_topic_id(chat_id=chat_id, msg_id=msg_id, root_id=root_id)

            inbound = self._make_inbound(
                chat_id=chat_id,
                user_id=sender_id,
                text=text,
                msg_type=msg_type,
                thread_ts=msg_id,
                files=files_list,
                metadata={
                    "message_id": msg_id,
                    "root_id": root_id,
                    "context_boundary": self._context_boundary,
                    "reply_in_thread": self._reply_in_thread,
                    "card_style": self._card_style,
                    "render_mode": self._render_mode,
                    "is_group_message": is_group_message,
                    "bot_mentioned": bot_mentioned,
                },
            )
            inbound.topic_id = topic_id

            # Schedule on the async event loop
            if self._main_loop and self._main_loop.is_running():
                logger.info("[Feishu] publishing inbound message to bus (type=%s, msg_id=%s)", msg_type.value, msg_id)
                fut = asyncio.run_coroutine_threadsafe(self._prepare_inbound(msg_id, inbound), self._main_loop)
                fut.add_done_callback(lambda f, mid=msg_id: self._log_future_error(f, "prepare_inbound", mid))
            else:
                logger.warning("[Feishu] main loop not running, cannot publish inbound message")
        except Exception:
            logger.exception("[Feishu] error processing message")
