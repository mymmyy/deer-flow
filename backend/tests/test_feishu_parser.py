import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.channels.commands import KNOWN_CHANNEL_COMMANDS
from app.channels.feishu import FeishuChannel
from app.channels.message_bus import InboundMessage, MessageBus


def _run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_feishu_on_message_plain_text():
    bus = MessageBus()
    config = {"app_id": "test", "app_secret": "test"}
    channel = FeishuChannel(bus, config)

    # Create mock event
    event = MagicMock()
    event.event.message.chat_id = "chat_1"
    event.event.message.message_id = "msg_1"
    event.event.message.root_id = None
    event.event.sender.sender_id.open_id = "user_1"

    # Plain text content
    content_dict = {"text": "Hello world"}
    event.event.message.content = json.dumps(content_dict)

    # Call _on_message
    channel._on_message(event)

    # Since main_loop isn't running in this synchronous test, we can't easily assert on bus,
    # but we can intercept _make_inbound to check the parsed text.
    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()
        assert mock_make_inbound.call_args[1]["text"] == "Hello world"


def test_feishu_on_message_rich_text():
    bus = MessageBus()
    config = {"app_id": "test", "app_secret": "test"}
    channel = FeishuChannel(bus, config)

    # Create mock event
    event = MagicMock()
    event.event.message.chat_id = "chat_1"
    event.event.message.message_id = "msg_1"
    event.event.message.root_id = None
    event.event.sender.sender_id.open_id = "user_1"

    # Rich text content (topic group / post)
    content_dict = {"content": [[{"tag": "text", "text": "Paragraph 1, part 1."}, {"tag": "text", "text": "Paragraph 1, part 2."}], [{"tag": "at", "text": "@bot"}, {"tag": "text", "text": " Paragraph 2."}]]}
    event.event.message.content = json.dumps(content_dict)

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()
        parsed_text = mock_make_inbound.call_args[1]["text"]

        # Expected text:
        # Paragraph 1, part 1. Paragraph 1, part 2.
        #
        # @bot  Paragraph 2.
        assert "Paragraph 1, part 1. Paragraph 1, part 2." in parsed_text
        assert "@bot  Paragraph 2." in parsed_text
        assert "\n\n" in parsed_text


def test_feishu_receive_file_replaces_placeholders_in_order():
    async def go():
        bus = MessageBus()
        channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})

        msg = InboundMessage(
            channel_name="feishu",
            chat_id="chat_1",
            user_id="user_1",
            text="before [image] middle [file] after",
            thread_ts="msg_1",
            files=[{"image_key": "img_key"}, {"file_key": "file_key"}],
        )

        channel._receive_single_file = AsyncMock(side_effect=["/mnt/user-data/uploads/a.png", "/mnt/user-data/uploads/b.pdf"])

        result = await channel.receive_file(msg, "thread_1")

        assert result.text == "before /mnt/user-data/uploads/a.png middle /mnt/user-data/uploads/b.pdf after"

    _run(go())


def test_feishu_on_message_extracts_image_and_file_keys():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})

    event = MagicMock()
    event.event.message.chat_id = "chat_1"
    event.event.message.message_id = "msg_1"
    event.event.message.root_id = None
    event.event.sender.sender_id.open_id = "user_1"

    # Rich text with one image and one file element.
    event.event.message.content = json.dumps(
        {
            "content": [
                [
                    {"tag": "text", "text": "See"},
                    {"tag": "img", "image_key": "img_123"},
                    {"tag": "file", "file_key": "file_456"},
                ]
            ]
        }
    )

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()
        files = mock_make_inbound.call_args[1]["files"]
        assert files == [{"image_key": "img_123"}, {"file_key": "file_456"}]
        assert "[image]" in mock_make_inbound.call_args[1]["text"]
        assert "[file]" in mock_make_inbound.call_args[1]["text"]


@pytest.mark.parametrize("command", sorted(KNOWN_CHANNEL_COMMANDS))
def test_feishu_recognizes_all_known_slash_commands(command):
    """Every entry in KNOWN_CHANNEL_COMMANDS must be classified as a command."""
    bus = MessageBus()
    config = {"app_id": "test", "app_secret": "test"}
    channel = FeishuChannel(bus, config)

    event = MagicMock()
    event.event.message.chat_id = "chat_1"
    event.event.message.message_id = "msg_1"
    event.event.message.root_id = None
    event.event.sender.sender_id.open_id = "user_1"
    event.event.message.content = json.dumps({"text": command})

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()
        assert mock_make_inbound.call_args[1]["msg_type"].value == "command", f"{command!r} should be classified as COMMAND"


@pytest.mark.parametrize(
    "text",
    [
        "/unknown",
        "/mnt/user-data/outputs/prd/technical-design.md",
        "/etc/passwd",
        "/not-a-command at all",
    ],
)
def test_feishu_treats_unknown_slash_text_as_chat(text):
    """Slash-prefixed text that is not a known command must be classified as CHAT."""
    bus = MessageBus()
    config = {"app_id": "test", "app_secret": "test"}
    channel = FeishuChannel(bus, config)

    event = MagicMock()
    event.event.message.chat_id = "chat_1"
    event.event.message.message_id = "msg_1"
    event.event.message.root_id = None
    event.event.sender.sender_id.open_id = "user_1"
    event.event.message.content = json.dumps({"text": text})

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()
        assert mock_make_inbound.call_args[1]["msg_type"].value == "chat", f"{text!r} should be classified as CHAT"


def _build_text_event(*, chat_id: str = "chat_1", msg_id: str = "msg_1", root_id: str | None = None, text: str = "hi"):
    event = MagicMock()
    event.event.message.chat_id = chat_id
    event.event.message.message_id = msg_id
    event.event.message.root_id = root_id
    event.event.sender.sender_id.open_id = "user_1"
    event.event.message.content = json.dumps({"text": text})
    return event


def test_feishu_context_boundary_chat_uses_chat_id_as_topic():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "context_boundary": "chat"})
    event = _build_text_event(chat_id="chat_a", msg_id="msg_a", root_id="root_a")

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        inbound = mock_make_inbound.return_value
        assert inbound.topic_id == "chat_a"


def test_feishu_context_boundary_group_is_alias_of_chat():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "context_boundary": "group"})
    event = _build_text_event(chat_id="chat_g", msg_id="msg_g", root_id="root_g")

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        inbound = mock_make_inbound.return_value
        assert inbound.topic_id == "chat_g"


def test_feishu_context_boundary_topic_uses_root_or_msg_id():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "context_boundary": "topic"})

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)

        channel._on_message(_build_text_event(chat_id="chat_t", msg_id="msg_t1", root_id="root_t"))
        inbound1 = mock_make_inbound.return_value
        assert inbound1.topic_id == "root_t"

        mock_make_inbound.reset_mock()
        channel._on_message(_build_text_event(chat_id="chat_t", msg_id="msg_t2", root_id=None))
        inbound2 = mock_make_inbound.return_value
        assert inbound2.topic_id == "msg_t2"


def test_feishu_invalid_context_boundary_fallbacks_to_chat():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "context_boundary": "invalid_value"})
    event = _build_text_event(chat_id="chat_f", msg_id="msg_f", root_id="root_f")

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        inbound = mock_make_inbound.return_value
        assert inbound.topic_id == "chat_f"


def test_feishu_reply_in_thread_config_is_used_for_reply():
    async def go():
        bus = MessageBus()
        channel = FeishuChannel(
            bus,
            {"app_id": "test", "app_secret": "test", "reply_in_thread": False},
        )

        class _Builder:
            def __init__(self):
                self.reply_in_thread_value = None
                self.payload = {}

            def msg_type(self, value):
                self.payload["msg_type"] = value
                return self

            def content(self, value):
                self.payload["content"] = value
                return self

            def reply_in_thread(self, value):
                self.reply_in_thread_value = value
                self.payload["reply_in_thread"] = value
                return self

            def build(self):
                return self.payload

        class _ReplyBodyFactory:
            def __init__(self):
                self.last_builder = None

            def builder(self):
                self.last_builder = _Builder()
                return self.last_builder

        class _ReplyRequestBuilder:
            def __init__(self):
                self.payload = {}

            def message_id(self, value):
                self.payload["message_id"] = value
                return self

            def request_body(self, value):
                self.payload["request_body"] = value
                return self

            def build(self):
                return self.payload

        class _ReplyRequestFactory:
            @staticmethod
            def builder():
                return _ReplyRequestBuilder()

        response = MagicMock()
        response.data.message_id = "card_1"
        response.success.return_value = True
        channel._api_client = MagicMock()
        channel._api_client.im.v1.message.reply = MagicMock(return_value=response)
        channel._ReplyMessageRequest = _ReplyRequestFactory
        body_factory = _ReplyBodyFactory()
        channel._ReplyMessageRequestBody = body_factory

        await channel._reply_card("msg_source", {"elements": [{"tag": "markdown", "content": "hello"}]})

        assert body_factory.last_builder is not None
        assert body_factory.last_builder.reply_in_thread_value is False

    _run(go())


def test_feishu_render_mode_auto_prefers_text_for_plain_response():
    bus = MessageBus()
    channel = FeishuChannel(
        bus,
        {"app_id": "test", "app_secret": "test", "render_mode": "auto"},
    )
    outbound = MagicMock()
    outbound.thread_ts = "msg_plain"
    outbound.metadata = {}
    outbound.text = "just a plain answer"

    assert channel._resolve_render_mode_for_message(outbound) == "text"


def test_feishu_render_mode_auto_prefers_card_for_table_like_response():
    bus = MessageBus()
    channel = FeishuChannel(
        bus,
        {"app_id": "test", "app_secret": "test", "render_mode": "auto"},
    )
    outbound = MagicMock()
    outbound.thread_ts = "msg_table"
    outbound.metadata = {}
    outbound.text = "| 日期 | 花费 |\n| --- | --- |\n| 2026-04-20 | 100 |"

    assert channel._resolve_render_mode_for_message(outbound) == "card"


def test_feishu_render_mode_auto_prefers_card_for_structured_payload():
    bus = MessageBus()
    channel = FeishuChannel(
        bus,
        {"app_id": "test", "app_secret": "test", "render_mode": "auto"},
    )
    outbound = MagicMock()
    outbound.thread_ts = "msg_payload"
    outbound.metadata = {"feishu_card_payload": {"title": "日报"}}
    outbound.text = "summary"

    assert channel._resolve_render_mode_for_message(outbound) == "card"


def test_feishu_render_mode_invalid_value_fallbacks_to_auto():
    bus = MessageBus()
    channel = FeishuChannel(
        bus,
        {"app_id": "test", "app_secret": "test", "render_mode": "invalid"},
    )
    outbound = MagicMock()
    outbound.thread_ts = "msg_invalid"
    outbound.metadata = {}
    outbound.text = "plain text"

    assert channel._resolve_render_mode_for_message(outbound) == "text"


def test_feishu_rejects_invalid_feishu_card_payload_schema():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {"feishu_card_payload": {"table": {"columns": ["日期"], "rows": "not-a-list"}}}
    outbound.text = "x"

    with pytest.raises(ValueError, match="rows must be array of rows"):
        channel._resolve_card(outbound)


def test_feishu_rejects_invalid_native_card_schema():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {"native_card": {"foo": "bar"}}
    outbound.text = "x"

    with pytest.raises(ValueError, match="native_card must include"):
        channel._resolve_card(outbound)


def test_feishu_send_falls_back_to_text_when_card_delivery_fails():
    async def go():
        bus = MessageBus()
        channel = FeishuChannel(
            bus,
            {"app_id": "test", "app_secret": "test", "render_mode": "card"},
        )
        channel._api_client = MagicMock()
        channel._send_card_message = AsyncMock(side_effect=RuntimeError("card failed"))
        channel._send_text_message = AsyncMock(return_value=None)

        msg = MagicMock()
        msg.chat_id = "chat_1"
        msg.thread_ts = "msg_1"
        msg.text = "final answer"
        msg.is_final = True
        msg.metadata = {}

        await channel.send(msg, _max_retries=1)
        channel._send_text_message.assert_awaited_once()

    _run(go())


def test_feishu_send_skips_non_final_text_message_update():
    async def go():
        bus = MessageBus()
        channel = FeishuChannel(
            bus,
            {"app_id": "test", "app_secret": "test", "render_mode": "text"},
        )
        channel._api_client = MagicMock()
        channel._send_text_message = AsyncMock(return_value=None)
        channel._send_card_message = AsyncMock(return_value=None)

        msg = MagicMock()
        msg.chat_id = "chat_1"
        msg.thread_ts = "msg_1"
        msg.text = "partial"
        msg.is_final = False
        msg.metadata = {}

        await channel.send(msg, _max_retries=1)
        channel._send_text_message.assert_not_awaited()
        channel._send_card_message.assert_not_awaited()

    _run(go())


def test_feishu_send_card_message_final_update_failure_falls_back_to_reply():
    async def go():
        bus = MessageBus()
        channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
        channel._running_card_ids = {"msg_1": "card_1"}
        channel._update_card = AsyncMock(side_effect=RuntimeError("patch failed"))
        channel._reply_card = AsyncMock(return_value="card_fallback")
        channel._add_reaction = AsyncMock(return_value=None)

        msg = MagicMock()
        msg.thread_ts = "msg_1"
        msg.chat_id = "chat_1"
        msg.text = "final"
        msg.is_final = True
        msg.metadata = {}

        await channel._send_card_message(msg)

        channel._update_card.assert_awaited_once()
        channel._reply_card.assert_awaited_once()
        channel._add_reaction.assert_awaited_once_with("msg_1", "DONE")
        assert "msg_1" not in channel._running_card_ids

    _run(go())


def test_feishu_send_card_message_non_final_waited_task_without_card_skips_duplicate_create():
    async def go():
        bus = MessageBus()
        channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
        fut = asyncio.Future()
        fut.set_result(None)
        channel._running_card_tasks = {"msg_2": fut}
        channel._ensure_running_card = AsyncMock(return_value=None)
        channel._update_card = AsyncMock(return_value=None)

        msg = MagicMock()
        msg.thread_ts = "msg_2"
        msg.chat_id = "chat_1"
        msg.text = "partial"
        msg.is_final = False
        msg.metadata = {}

        await channel._send_card_message(msg)

        channel._ensure_running_card.assert_not_awaited()
        channel._update_card.assert_not_awaited()

    _run(go())


def test_feishu_send_card_message_non_final_without_running_card_creates_running_card():
    async def go():
        bus = MessageBus()
        channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
        channel._ensure_running_card = AsyncMock(return_value="card_3")
        channel._update_card = AsyncMock(return_value=None)

        msg = MagicMock()
        msg.thread_ts = "msg_3"
        msg.chat_id = "chat_1"
        msg.text = "partial"
        msg.is_final = False
        msg.metadata = {}

        await channel._send_card_message(msg)

        channel._ensure_running_card.assert_awaited_once_with("msg_3", "partial")
        channel._update_card.assert_not_awaited()

    _run(go())


def test_feishu_build_progress_card_contains_status_and_events():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})

    card = channel._build_progress_card(
        "partial output",
        {
            "status_stage": "调用工具中",
            "progress_events": [
                {"stage": "解析需求", "detail": "识别用户问题"},
                {"stage": "调用工具中", "detail": "web_search(q=花费趋势)"},
            ],
        },
    )

    assert card["header"]["title"]["content"].startswith("DeerFlow")
    markdown_blocks = [el["content"] for el in card["elements"] if el.get("tag") == "markdown"]
    merged = "\n".join(markdown_blocks)
    assert "当前状态" in merged
    assert "执行明细" in merged
    assert "web_search" in merged


def test_feishu_send_card_message_non_final_uses_progress_card_when_metadata_present():
    async def go():
        bus = MessageBus()
        channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
        channel._running_card_ids = {"msg_progress": "card_progress"}
        channel._update_card = AsyncMock(return_value=None)

        msg = MagicMock()
        msg.thread_ts = "msg_progress"
        msg.chat_id = "chat_1"
        msg.text = "partial"
        msg.is_final = False
        msg.metadata = {
            "status_stage": "调用工具中",
            "progress_events": [{"stage": "调用工具中", "detail": "read_file(path=report.md)"}],
        }

        await channel._send_card_message(msg)

        channel._update_card.assert_awaited_once()
        called_card = channel._update_card.await_args.args[1]
        markdown_blocks = [el["content"] for el in called_card["elements"] if el.get("tag") == "markdown"]
        merged = "\n".join(markdown_blocks)
        assert "当前状态" in merged
        assert "执行明细" in merged
        assert "read_file" in merged

    _run(go())


def test_feishu_markdown_table_multi_dimension_renders_table_card():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {}
    outbound.text = (
        "| 日期 | 渠道 | 花费 |\n"
        "| --- | --- | --- |\n"
        "| 2026-04-20 | 抖音 | 100 |\n"
        "| 2026-04-20 | 快手 | 80 |"
    )

    card = channel._resolve_card(outbound)
    assert card["elements"][0]["tag"] == "markdown"
    assert "多维数据表" in card["header"]["title"]["content"]


def test_feishu_markdown_table_single_dimension_time_uses_line_chart():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {}
    outbound.text = (
        "| 日期 | 花费 |\n"
        "| --- | --- |\n"
        "| 2026-04-20 | 100 |\n"
        "| 2026-04-21 | 120 |"
    )

    card = channel._resolve_card(outbound)
    assert card["elements"][0]["tag"] == "chart"
    spec = card["elements"][0]["chart_spec"]
    assert spec["type"] == "line"
    assert spec["series"][0]["name"] == "花费"


def test_feishu_markdown_table_single_dimension_non_time_uses_bar_chart():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {}
    outbound.text = (
        "| 渠道 | 花费 |\n"
        "| --- | --- |\n"
        "| 抖音 | 100 |\n"
        "| 快手 | 80 |"
    )

    card = channel._resolve_card(outbound)
    assert card["elements"][0]["tag"] == "chart"
    spec = card["elements"][0]["chart_spec"]
    assert spec["type"] == "bar"
    assert spec["xAxis"]["name"] == "渠道"


def test_feishu_markdown_table_single_dimension_multi_metric_keeps_multi_series():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {}
    outbound.text = (
        "| 日期 | 花费 | 点击 |\n"
        "| --- | --- | --- |\n"
        "| 2026-04-20 | 100 | 20 |\n"
        "| 2026-04-21 | 120 | 25 |"
    )

    card = channel._resolve_card(outbound)
    assert card["elements"][0]["tag"] == "chart"
    spec = card["elements"][0]["chart_spec"]
    assert len(spec["series"]) == 2
    assert spec["series"][0]["type"] == "line"


def test_feishu_legacy_line_chart_payload_normalizes_to_chart_spec():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {
        "feishu_card_payload": {
            "title": "趋势",
            "line_chart": {
                "x": ["2026-04-20", "2026-04-21"],
                "y": [100, 120],
                "name": "花费",
            },
        }
    }
    outbound.text = "x"

    card = channel._resolve_card(outbound)
    chart_elements = [el for el in card["elements"] if el.get("tag") == "chart"]
    assert chart_elements
    spec = chart_elements[0]["chart_spec"]
    assert spec["type"] == "line"
    assert spec["series"][0]["name"] == "花费"
    assert spec["series"][0]["data"] == [100, 120]


def test_feishu_legacy_bar_chart_payload_normalizes_to_chart_spec():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {
        "bar_chart": {
            "categories": ["抖音", "快手"],
            "series": [{"name": "花费", "data": [100, 80]}],
        }
    }
    outbound.text = "x"

    card = channel._resolve_card(outbound)
    assert card["elements"][0]["tag"] == "chart"
    spec = card["elements"][0]["chart_spec"]
    assert spec["type"] == "bar"
    assert spec["xAxis"]["data"] == ["抖音", "快手"]


def test_feishu_legacy_chart_data_payload_normalizes_to_chart_spec():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {
        "chart_data": {
            "type": "line",
            "labels": ["Mon", "Tue"],
            "series": {"点击": [10, 20]},
        }
    }
    outbound.text = "x"

    card = channel._resolve_card(outbound)
    assert card["elements"][0]["tag"] == "chart"
    spec = card["elements"][0]["chart_spec"]
    assert spec["type"] == "line"
    assert spec["series"][0]["name"] == "点击"
    assert spec["series"][0]["data"] == [10, 20]
