"""Feishu/Lark channel - connects to Feishu via WebSocket (no public IP needed)."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import threading
from typing import Any, Literal

from app.channels.base import Channel
from app.channels.commands import KNOWN_CHANNEL_COMMANDS
from app.channels.message_bus import InboundMessage, InboundMessageType, MessageBus, OutboundMessage, ResolvedAttachment
from deerflow.config.paths import VIRTUAL_PATH_PREFIX, get_paths
from deerflow.sandbox.sandbox_provider import get_sandbox_provider

logger = logging.getLogger(__name__)

_ALLOWED_CONTEXT_BOUNDARIES = {"chat", "group", "topic"}
_ALLOWED_CARD_STYLES = {"markdown", "rich"}
_ALLOWED_RENDER_MODES = {"auto", "card", "text"}
_TIME_KEYWORDS = ("date", "time", "day", "week", "month", "year", "日期", "时间", "日", "周", "月", "年")


def _is_feishu_command(text: str) -> bool:
    if not text.startswith("/"):
        return False
    return text.split(maxsplit=1)[0].lower() in KNOWN_CHANNEL_COMMANDS


class FeishuChannel(Channel):
    """Feishu/Lark IM channel using the ``lark-oapi`` WebSocket client.

    Configuration keys (in ``config.yaml`` under ``channels.feishu``):
        - ``app_id``: Feishu app ID.
        - ``app_secret``: Feishu app secret.
        - ``verification_token``: (optional) Event verification token.

    The channel uses WebSocket long-connection mode so no public IP is required.

    Message flow:
        1. User sends a message 鈫?bot adds "OK" emoji reaction
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
        logger.info("[Feishu] config loaded: render_mode=%s", self._render_mode)

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
    def _has_markdown_table(text: str) -> bool:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if len(lines) < 2:
            return False
        for idx in range(len(lines) - 1):
            if "|" not in lines[idx] or "|" not in lines[idx + 1]:
                continue
            if re.search(r"^\|?[\s:-]+\|[\s|:-]*\|?$", lines[idx + 1]):
                return True
        return False

    def _is_visual_content(self, msg: OutboundMessage) -> bool:
        metadata = msg.metadata if isinstance(msg.metadata, dict) else {}
        if isinstance(metadata.get("native_card"), dict):
            return True
        if isinstance(metadata.get("feishu_card_payload"), dict):
            return True
        if any(key in metadata for key in ("line_chart", "bar_chart", "chart_spec", "chart_data")):
            return True

        text = msg.text or ""
        if self._has_markdown_table(text):
            return True

        lowered = text.lower()
        signal_terms = (
            "line chart",
            "bar chart",
            "chart_spec",
        )
        return any(term in lowered for term in signal_terms)

    def _resolve_render_mode_for_message(self, msg: OutboundMessage) -> str:
        source_message_id = msg.thread_ts
        if source_message_id and source_message_id in self._message_render_modes:
            return self._message_render_modes[source_message_id]

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
    def _parse_markdown_table(text: str) -> tuple[list[str], list[list[str]]] | None:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for idx in range(len(lines) - 1):
            header_line = lines[idx]
            divider_line = lines[idx + 1]
            if "|" not in header_line or "|" not in divider_line:
                continue
            if not re.match(r"^\|?[\s:-]+\|[\s|:-]*\|?$", divider_line):
                continue

            def parse_row(row: str) -> list[str]:
                raw = row.strip()
                if raw.startswith("|"):
                    raw = raw[1:]
                if raw.endswith("|"):
                    raw = raw[:-1]
                return [cell.strip() for cell in raw.split("|")]

            columns = parse_row(header_line)
            rows: list[list[str]] = []
            for row_line in lines[idx + 2 :]:
                if "|" not in row_line:
                    break
                row = parse_row(row_line)
                if len(row) < len(columns):
                    row.extend([""] * (len(columns) - len(row)))
                rows.append(row[: len(columns)])

            if columns and rows:
                return columns, rows
        return None

    @staticmethod
    def _parse_number(raw: str) -> float | None:
        value = raw.strip().replace(",", "")
        if not value:
            return None
        if value.endswith("%"):
            value = value[:-1]
        try:
            return float(value)
        except ValueError:
            return None

    @staticmethod
    def _is_time_dimension(column: str) -> bool:
        lowered = column.strip().lower()
        return any(keyword in lowered for keyword in _TIME_KEYWORDS)

    def _infer_table_schema(self, columns: list[str], rows: list[list[str]]) -> tuple[list[int], list[int]]:
        metric_indexes: list[int] = []
        for col_idx, _ in enumerate(columns):
            numeric_cells = 0
            total_cells = 0
            for row in rows:
                if col_idx >= len(row):
                    continue
                total_cells += 1
                if self._parse_number(row[col_idx]) is not None:
                    numeric_cells += 1
            if total_cells > 0 and numeric_cells / total_cells >= 0.6:
                metric_indexes.append(col_idx)

        dimension_indexes = [idx for idx in range(len(columns)) if idx not in metric_indexes]
        if not dimension_indexes and columns:
            dimension_indexes = [0]
            if 0 in metric_indexes:
                metric_indexes.remove(0)
        return dimension_indexes, metric_indexes

    @staticmethod
    def _build_table_card(columns: list[str], rows: list[list[str]], title: str = "数据表") -> dict[str, Any]:
        markdown_table = FeishuChannel._build_markdown_table(columns, rows)
        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "header": {"title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "markdown", "content": markdown_table}],
        }

    def _build_chart_card(
        self,
        *,
        chart_type: str,
        dimension_index: int,
        metric_indexes: list[int],
        columns: list[str],
        rows: list[list[str]],
    ) -> dict[str, Any]:
        categories = [row[dimension_index] if dimension_index < len(row) else "" for row in rows]
        series = []
        for metric_idx in metric_indexes:
            data = []
            for row in rows:
                cell = row[metric_idx] if metric_idx < len(row) else ""
                number = self._parse_number(cell)
                data.append(number if number is not None else 0)
            series.append({"name": columns[metric_idx], "type": chart_type, "data": data})

        chart_spec = {
            "type": chart_type,
            "xAxis": {"type": "category", "data": categories, "name": columns[dimension_index]},
            "yAxis": {"type": "value"},
            "series": series,
            "legend": {"show": len(series) > 1},
        }

        title = "趋势图" if chart_type == "line" else "柱状图"
        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "header": {"title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "chart", "chart_spec": chart_spec}],
        }

    def _build_card_from_markdown_table(self, text: str) -> dict[str, Any] | None:
        parsed = self._parse_markdown_table(text)
        if parsed is None:
            return None
        columns, rows = parsed
        dimension_indexes, metric_indexes = self._infer_table_schema(columns, rows)

        # Multi-dimension: always render as table.
        if len(dimension_indexes) > 1:
            return self._build_table_card(columns, rows, title="多维数据表")

        # No metric columns: keep table.
        if not metric_indexes:
            return self._build_table_card(columns, rows, title="数据表")

        # Single-dimension chart selection:
        # - time-like dimension -> line chart
        # - otherwise -> bar chart
        dimension_idx = dimension_indexes[0]
        is_time_dimension = self._is_time_dimension(columns[dimension_idx])
        chart_type = "line" if is_time_dimension else "bar"
        return self._build_chart_card(
            chart_type=chart_type,
            dimension_index=dimension_idx,
            metric_indexes=metric_indexes,
            columns=columns,
            rows=rows,
        )

    @staticmethod
    def _validate_native_card(native_card: dict[str, Any]) -> None:
        if not any(key in native_card for key in ("elements", "header", "config")):
            raise ValueError("native_card must include at least one of: elements, header, config")
        try:
            json.dumps(native_card)
        except (TypeError, ValueError) as exc:
            raise ValueError("native_card is not JSON serializable") from exc

    @staticmethod
    def _validate_feishu_card_payload(payload: dict[str, Any]) -> None:
        title = payload.get("title")
        if title is not None and not isinstance(title, str):
            raise ValueError("feishu_card_payload.title must be a string")

        summary = payload.get("summary")
        if summary is not None and not isinstance(summary, str):
            raise ValueError("feishu_card_payload.summary must be a string")

        table = payload.get("table")
        if table is not None:
            if not isinstance(table, dict):
                raise ValueError("feishu_card_payload.table must be an object")
            columns = table.get("columns")
            rows = table.get("rows")
            if not isinstance(columns, list) or not all(isinstance(col, str) for col in columns):
                raise ValueError("feishu_card_payload.table.columns must be string[]")
            if not isinstance(rows, list) or not all(isinstance(row, list) for row in rows):
                raise ValueError("feishu_card_payload.table.rows must be array of rows")

        for key in ("chart_spec", "line_chart", "native_card"):
            value = payload.get(key)
            if value is not None and not isinstance(value, dict):
                raise ValueError(f"feishu_card_payload.{key} must be an object")

    @staticmethod
    def _normalize_chart_series(
        series_raw: Any,
        *,
        chart_type: str,
        default_name: str = "value",
    ) -> list[dict[str, Any]]:
        if isinstance(series_raw, list):
            if not series_raw:
                return []
            if all(isinstance(item, (int, float)) for item in series_raw):
                return [{"name": default_name, "type": chart_type, "data": list(series_raw)}]
            normalized: list[dict[str, Any]] = []
            for item in series_raw:
                if not isinstance(item, dict):
                    continue
                data = item.get("data")
                if not isinstance(data, list):
                    continue
                normalized.append(
                    {
                        "name": str(item.get("name") or default_name),
                        "type": str(item.get("type") or chart_type),
                        "data": data,
                    }
                )
            return normalized

        if isinstance(series_raw, dict):
            normalized = []
            for key, values in series_raw.items():
                if isinstance(values, list):
                    normalized.append(
                        {
                            "name": str(key),
                            "type": chart_type,
                            "data": values,
                        }
                    )
            return normalized

        return []

    @classmethod
    def _normalize_legacy_chart_spec(cls, raw: dict[str, Any], *, default_type: str) -> dict[str, Any] | None:
        # New format passthrough.
        chart_spec = raw.get("chart_spec")
        if isinstance(chart_spec, dict):
            return chart_spec

        categories = raw.get("categories")
        if not isinstance(categories, list):
            categories = raw.get("x")
        if not isinstance(categories, list):
            categories = raw.get("labels")
        if not isinstance(categories, list):
            categories = []

        series_raw = raw.get("series")
        if series_raw is None:
            y_value = raw.get("y")
            if isinstance(y_value, list) and all(isinstance(item, (int, float)) for item in y_value):
                series_raw = [{"name": str(raw.get("name") or "value"), "data": y_value}]
            else:
                series_raw = y_value

        chart_type = str(raw.get("type") or default_type)
        series = cls._normalize_chart_series(
            series_raw,
            chart_type=chart_type,
            default_name=str(raw.get("name") or "value"),
        )
        if not series:
            return None

        return {
            "type": chart_type,
            "xAxis": {"type": "category", "data": categories},
            "yAxis": {"type": "value"},
            "series": series,
            "legend": {"show": len(series) > 1},
        }

    @classmethod
    def _normalize_legacy_chart_payload(cls, payload: dict[str, Any]) -> dict[str, Any] | None:
        spec = cls._normalize_legacy_chart_spec(payload, default_type="line")
        if spec is not None:
            return spec

        line_chart = payload.get("line_chart")
        if isinstance(line_chart, dict):
            spec = cls._normalize_legacy_chart_spec(line_chart, default_type="line")
            if spec is not None:
                return spec

        bar_chart = payload.get("bar_chart")
        if isinstance(bar_chart, dict):
            spec = cls._normalize_legacy_chart_spec(bar_chart, default_type="bar")
            if spec is not None:
                return spec

        chart_data = payload.get("chart_data")
        if isinstance(chart_data, dict):
            chart_type = str(chart_data.get("type") or payload.get("chart_type") or "line")
            spec = cls._normalize_legacy_chart_spec(chart_data, default_type=chart_type)
            if spec is not None:
                return spec

        return None

    def _build_card_from_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if isinstance(payload.get("native_card"), dict):
            self._validate_native_card(payload["native_card"])
            card = dict(payload["native_card"])
            card.setdefault("config", {})
            if isinstance(card["config"], dict):
                card["config"].setdefault("update_multi", True)
            return card

        elements: list[dict[str, Any]] = []
        title = payload.get("title")
        summary = payload.get("summary")
        if isinstance(title, str) and title.strip():
            elements.append({"tag": "markdown", "content": f"### {title.strip()}"})
        if isinstance(summary, str) and summary.strip():
            elements.append({"tag": "markdown", "content": summary.strip()})

        table = payload.get("table")
        if isinstance(table, dict):
            columns = table.get("columns")
            rows = table.get("rows")
            if isinstance(columns, list) and isinstance(rows, list):
                markdown_table = self._build_markdown_table(
                    [str(col) for col in columns],
                    [row if isinstance(row, list) else [row] for row in rows],
                )
                if markdown_table:
                    elements.append({"tag": "markdown", "content": markdown_table})

        chart_spec = self._normalize_legacy_chart_payload(payload)
        if isinstance(chart_spec, dict):
            elements.append({"tag": "chart", "chart_spec": chart_spec})

        if not elements:
            elements.append({"tag": "markdown", "content": payload.get("text", "") or " "})

        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "elements": elements,
        }

    def _resolve_card(self, msg: OutboundMessage) -> dict[str, Any]:
        metadata = msg.metadata if isinstance(msg.metadata, dict) else {}
        if isinstance(metadata.get("native_card"), dict):
            return self._build_card_from_payload({"native_card": metadata["native_card"]})
        if isinstance(metadata.get("feishu_card_payload"), dict):
            payload = metadata["feishu_card_payload"]
            self._validate_feishu_card_payload(payload)
            return self._build_card_from_payload(payload)
        if any(isinstance(metadata.get(key), dict) for key in ("line_chart", "bar_chart", "chart_data")):
            return self._build_card_from_payload(metadata)

        auto_table_card = self._build_card_from_markdown_table(msg.text or "")
        if auto_table_card is not None:
            return auto_table_card

        return self._build_text_card(msg.text)

    def _build_text_card(self, text: str) -> dict[str, Any]:
        card: dict[str, Any] = {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "elements": [{"tag": "markdown", "content": text}],
        }
        if self._card_style == "rich":
            card["header"] = {
                "title": {"tag": "plain_text", "content": "DeerFlow"},
            }
        return card

    @staticmethod
    def _build_progress_card(text: str, metadata: dict[str, Any]) -> dict[str, Any]:
        stage = metadata.get("status_stage")
        events = metadata.get("progress_events")
        elements: list[dict[str, Any]] = []

        if isinstance(stage, str) and stage.strip():
            elements.append({"tag": "markdown", "content": f"**当前状态**：{stage.strip()}"})

        if isinstance(events, list) and events:
            lines: list[str] = []
            for item in events[-8:]:
                if not isinstance(item, dict):
                    continue
                stage_label = str(item.get("stage") or "处理中")
                detail = str(item.get("detail") or "").strip()
                if detail:
                    lines.append(f"- {stage_label} · {detail}")
                else:
                    lines.append(f"- {stage_label}")
            if lines:
                elements.append({"tag": "markdown", "content": "**执行明细**\n" + "\n".join(lines)})

        if text:
            elements.append({"tag": "markdown", "content": f"**输出片段**\n{text}"})

        if not elements:
            elements.append({"tag": "markdown", "content": text or "正在处理中..."})

        return {
            "config": {"wide_screen_mode": True, "update_multi": True},
            "header": {"title": {"tag": "plain_text", "content": "DeerFlow · 实时进展"}},
            "elements": elements,
        }

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

            event_handler = lark.EventDispatcherHandler.builder("", "").register_p2_im_message_receive_v1(self._on_message).build()
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

    async def send(self, msg: OutboundMessage, *, _max_retries: int = 3) -> None:
        if not self._api_client:
            logger.warning("[Feishu] send called but no api_client available")
            return

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
                request = self._CreateMessageRequest.builder().receive_id_type("chat_id").request_body(self._CreateMessageRequestBody.builder().receive_id(msg.chat_id).msg_type(msg_type).content(content).build()).build()
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
        paths.ensure_thread_dirs(thread_id)
        uploads_dir = paths.sandbox_uploads_dir(thread_id).resolve()

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
        request = (
            self._CreateMessageRequest.builder()
            .receive_id_type("chat_id")
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
        request = self._CreateMessageRequest.builder().receive_id_type("chat_id").request_body(self._CreateMessageRequestBody.builder().receive_id(chat_id).msg_type("interactive").content(content).build()).build()
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

    def _ensure_running_card_started(self, source_message_id: str, text: str = "Working on it...") -> asyncio.Task | None:
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

    async def _ensure_running_card(self, source_message_id: str, text: str = "Working on it...") -> str | None:
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

    def _on_message(self, event) -> None:
        """Called by lark-oapi when a message is received (runs in lark thread)."""
        try:
            logger.info("[Feishu] raw event received: type=%s", type(event).__name__)
            message = event.event.message
            chat_id = message.chat_id
            msg_id = message.message_id
            sender_id = event.event.sender.sender_id.open_id

            # root_id is set when the message is a reply within a Feishu thread.
            # Use it as topic_id so all replies share the same DeerFlow thread.
            root_id = getattr(message, "root_id", None) or None

            # Parse message content
            content = json.loads(message.content)

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
                "[Feishu] parsed message: chat_id=%s, msg_id=%s, root_id=%s, sender=%s, text=%r",
                chat_id,
                msg_id,
                root_id,
                sender_id,
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


