import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.channels.commands import KNOWN_CHANNEL_COMMANDS
from app.channels.feishu_contract import validate_and_normalize_contract, validate_contract_quality
from app.channels.feishu_presentation_builder import (
    build_feishu_presentation_prompt,
    build_feishu_presentation_retry_feedback,
)
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


def _build_text_event(
    *,
    chat_id: str = "chat_1",
    msg_id: str = "msg_1",
    root_id: str | None = None,
    text: str = "hi",
    chat_type: str = "p2p",
):
    event = MagicMock()
    event.event.message.chat_id = chat_id
    event.event.message.message_id = msg_id
    event.event.message.root_id = root_id
    event.event.message.chat_type = chat_type
    event.event.sender.sender_id.open_id = "user_1"
    event.event.message.content = json.dumps({"text": text})
    return event


def _build_rich_text_event(
    *,
    chat_id: str = "chat_1",
    msg_id: str = "msg_1",
    root_id: str | None = None,
    content: dict,
    chat_type: str = "p2p",
):
    event = MagicMock()
    event.event.message.chat_id = chat_id
    event.event.message.message_id = msg_id
    event.event.message.root_id = root_id
    event.event.message.chat_type = chat_type
    event.event.sender.sender_id.open_id = "user_1"
    event.event.message.content = json.dumps(content)
    return event


def test_feishu_group_message_without_mention_is_ignored():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "require_mention_in_group": True})
    event = _build_text_event(text="normal team chatter", chat_type="group")

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_not_called()


def test_feishu_group_message_with_bot_mention_is_accepted_and_cleaned():
    bus = MessageBus()
    channel = FeishuChannel(
        bus,
        {
            "app_id": "test",
            "app_secret": "test",
            "require_mention_in_group": True,
            "bot_open_id": "ou_bot",
        },
    )
    event = _build_rich_text_event(
        chat_type="group",
        content={
            "content": [
                [
                    {"tag": "at", "user_id": "ou_bot", "text": "@DeerFlow"},
                    {"tag": "text", "text": " please summarize this"},
                ]
            ]
        },
    )

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()
        assert mock_make_inbound.call_args[1]["text"] == "please summarize this"
        assert mock_make_inbound.call_args[1]["metadata"]["is_group_message"] is True
        assert mock_make_inbound.call_args[1]["metadata"]["bot_mentioned"] is True


def test_feishu_group_mention_requires_configured_bot_identity():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "require_mention_in_group": True})
    event = _build_rich_text_event(
        chat_type="group",
        content={
            "content": [
                [
                    {"tag": "at", "user_id": "ou_someone_else", "text": "@Someone"},
                    {"tag": "text", "text": " this should not wake the bot"},
                ]
            ]
        },
    )

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_not_called()


def test_feishu_group_plain_text_placeholder_mention_is_accepted_and_cleaned():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "require_mention_in_group": True})
    event = _build_text_event(text="@_user_1 please summarize this", chat_type="group")

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()
        assert mock_make_inbound.call_args[1]["text"] == "please summarize this"


def test_feishu_group_known_command_without_mention_is_accepted():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "require_mention_in_group": True})
    command = sorted(KNOWN_CHANNEL_COMMANDS)[0]
    event = _build_text_event(text=command, chat_type="group")

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()
        assert mock_make_inbound.call_args[1]["msg_type"].value == "command"


def test_feishu_private_message_without_mention_is_accepted():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test", "require_mention_in_group": True})
    event = _build_text_event(text="hello", chat_type="p2p")

    with pytest.MonkeyPatch.context() as m:
        mock_make_inbound = MagicMock()
        m.setattr(channel, "_make_inbound", mock_make_inbound)
        channel._on_message(event)

        mock_make_inbound.assert_called_once()


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


def test_feishu_render_mode_auto_prefers_text_for_table_like_response_without_explicit_payload():
    bus = MessageBus()
    channel = FeishuChannel(
        bus,
        {"app_id": "test", "app_secret": "test", "render_mode": "auto"},
    )
    outbound = MagicMock()
    outbound.thread_ts = "msg_table"
    outbound.metadata = {}
    outbound.text = "| 鏃ユ湡 | 鑺辫垂 |\n| --- | --- |\n| 2026-04-20 | 100 |"

    assert channel._resolve_render_mode_for_message(outbound) == "text"


def test_feishu_render_mode_auto_prefers_card_for_explicit_chart_spec_payload():
    bus = MessageBus()
    channel = FeishuChannel(
        bus,
        {"app_id": "test", "app_secret": "test", "render_mode": "auto"},
    )
    outbound = MagicMock()
    outbound.thread_ts = "msg_payload"
    outbound.metadata = {"feishu_card_payload": {"title": "鏃ユ姤", "chart_spec": {"type": "line", "series": []}}}
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
    outbound.metadata = {"feishu_card_payload": {"title": "invalid"}}
    outbound.text = "x"

    with pytest.raises(ValueError, match="chart_spec must be an object"):
        channel._resolve_card(outbound)


def test_feishu_resolves_explicit_chart_spec_payload():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.metadata = {
        "feishu_card_payload": {
            "title": "trend",
            "summary": "daily spend",
            "chart_spec": {
                "type": "line",
                "xAxis": {"type": "category", "data": ["2026-04-20", "2026-04-21"]},
                "yAxis": {"type": "value"},
                "series": [{"name": "spend", "type": "line", "data": [100, 120]}],
            },
        }
    }
    outbound.text = "x"

    card = channel._resolve_card(outbound)
    chart_elements = [el for el in card["elements"] if el.get("tag") == "chart"]
    assert chart_elements
    assert chart_elements[0]["chart_spec"]["type"] == "line"


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
                {"stage": "解析需求", "detail": "识别用户意图"},
                {"stage": "调用工具中", "detail": "web_search(q=spend trend)"},
            ],
            "progress_timeline": [
                {"kind": "thought", "stage": "解析需求", "detail": "规划执行路径", "elapsed_seconds": 1},
                {"kind": "action", "stage": "调用工具中", "detail": "web_search(q=spend trend)", "elapsed_seconds": 3},
            ],
            "progress_elapsed_seconds": 3,
            "progress_timer_running": True,
        },
    )

    markdown_blocks = [el["content"] for el in card["elements"] if el.get("tag") == "markdown"]
    merged = "\n".join(markdown_blocks)
    assert "[00:03] 行动 | 调用工具中 | web_search(q=spend trend)" in merged
    assert "web_search" in merged
    assert "partial output (3s)" in merged


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
        assert "调用工具中" in merged
        assert "read_file" in merged
        assert "partial (0s)" in merged

    _run(go())


def test_feishu_resolves_skill_contract_chart_block_to_chart_element():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.channel_name = "feishu"
    outbound.metadata = {
        "feishu_skill_contract": {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "title": "report",
                "blocks": [
                    {
                        "type": "chart",
                        "chart": {
                            "chart_type": "line",
                            "dimension": {"name": "date", "values": ["2026-04-20", "2026-04-21"]},
                            "metrics": [{"name": "spend", "values": [100, 120]}],
                        },
                    }
                ],
            },
        }
    }
    outbound.text = "fallback"

    card = channel._resolve_card(outbound)
    chart_elements = [el for el in card["elements"] if el.get("tag") == "chart"]
    assert chart_elements
    assert chart_elements[0]["chart_spec"]["type"] == "line"
    assert isinstance(chart_elements[0]["chart_spec"].get("data"), dict)
    assert isinstance(chart_elements[0]["chart_spec"]["data"].get("values"), list)
    assert chart_elements[0]["chart_spec"].get("xField") == "dimension"
    assert chart_elements[0]["chart_spec"].get("yField") == "value"


def test_feishu_render_mode_auto_upgrades_text_cache_to_card_when_contract_arrives():
    bus = MessageBus()
    channel = FeishuChannel(
        bus,
        {"app_id": "test", "app_secret": "test", "render_mode": "auto"},
    )

    # Simulate streaming: first chunk has plain text and gets cached as text mode.
    chunk = MagicMock()
    chunk.thread_ts = "msg_upgrade"
    chunk.metadata = {}
    chunk.text = "processing..."
    assert channel._resolve_render_mode_for_message(chunk) == "text"

    # Final chunk carries explicit Feishu skill contract and should upgrade to card.
    final_msg = MagicMock()
    final_msg.thread_ts = "msg_upgrade"
    final_msg.metadata = {
        "feishu_skill_contract": {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "blocks": [
                    {
                        "type": "chart",
                        "chart": {
                            "chart_type": "line",
                            "dimension": {"name": "date", "values": ["2026-04-20", "2026-04-21"]},
                            "metrics": [{"name": "spend", "values": [100, 120]}],
                        },
                    }
                ]
            },
        }
    }
    final_msg.text = "done"

    assert channel._resolve_render_mode_for_message(final_msg) == "card"


def test_feishu_resolves_embedded_metadata_contract_json_text_to_chart_card():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.channel_name = "feishu"
    outbound.metadata = {}
    outbound.text = (
        '{"metadata":{"feishu_skill_contract":{"card_schema_version":"v1","target_channel":"feishu",'
        '"render_mode":"card","fallback_text":"fallback","card_payload":{"title":"trend","blocks":[{"type":"chart",'
        '"chart":{"chart_type":"line","dimension":{"name":"date","values":["04-21","04-22"]},'
        '"metrics":[{"name":"spend","values":[100,120]}]}}]}}}}'
    )

    card = channel._resolve_card(outbound)
    chart_elements = [el for el in card["elements"] if el.get("tag") == "chart"]
    assert chart_elements
    assert chart_elements[0]["chart_spec"]["type"] == "line"
    assert isinstance(chart_elements[0]["chart_spec"].get("data"), dict)


def test_feishu_resolves_skill_contract_image_block():
    bus = MessageBus()
    channel = FeishuChannel(bus, {"app_id": "test", "app_secret": "test"})
    outbound = MagicMock()
    outbound.channel_name = "feishu"
    outbound.metadata = {
        "feishu_skill_contract": {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "title": "report",
                "summary": "summary",
                "blocks": [
                    {"type": "markdown", "markdown": "### 结论\n见下图。"},
                    {"type": "image", "image": {"image_key": "img_test", "caption": "趋势附图"}},
                ],
            },
        }
    }
    outbound.text = "fallback"

    card = channel._resolve_card(outbound)
    img_elements = [el for el in card["elements"] if el.get("tag") == "img"]
    markdown_elements = [el for el in card["elements"] if el.get("tag") == "markdown"]
    assert img_elements
    assert img_elements[0]["img_key"] == "img_test"
    assert any(el.get("content") == "趋势附图" for el in markdown_elements)


def test_feishu_contract_accepts_image_block_and_chart_quality_fields():
    result = validate_and_normalize_contract(
        {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "title": "report",
                "summary": "summary",
                "blocks": [
                    {"type": "markdown", "markdown": "### 结论\n整体上升。"},
                    {
                        "type": "chart",
                        "chart": {
                            "chart_type": "line",
                            "title": "趋势",
                            "why_this_chart": "展示趋势",
                            "dimension": {"name": "date", "values": ["04-21", "04-22"]},
                            "metrics": [{"name": "spend", "values": [100, 120]}],
                        },
                    },
                    {
                        "type": "image",
                        "image": {
                            "image_key": "img_xxx",
                            "caption": "附图说明",
                        },
                    },
                ],
            },
        },
        channel_name="feishu",
    )

    assert result.ok is True
    assert result.normalized is not None
    assert result.normalized["card_payload"]["blocks"][1]["chart"]["why_this_chart"] == "展示趋势"
    assert result.normalized["card_payload"]["blocks"][2]["image"]["image_key"] == "img_xxx"


def test_feishu_contract_quality_rejects_chart_only_card():
    result = validate_contract_quality(
        {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "title": "report",
                "summary": "summary",
                "blocks": [
                    {
                        "type": "chart",
                        "chart": {
                            "chart_type": "line",
                            "title": "趋势",
                            "why_this_chart": "展示趋势",
                            "dimension": {"name": "date", "values": ["04-21", "04-22"]},
                            "metrics": [{"name": "spend", "values": [100, 120]}],
                        },
                    }
                ],
            },
        }
    )

    assert result.ok is False
    assert "card must contain at least 2 blocks" in result.errors
    assert "card must contain at least 1 markdown conclusion block" in result.errors


def test_feishu_contract_quality_rejects_chart_and_table_together():
    result = validate_contract_quality(
        {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "title": "report",
                "summary": "summary",
                "blocks": [
                    {"type": "markdown", "markdown": "结论"},
                    {
                        "type": "chart",
                        "chart": {
                            "chart_type": "line",
                            "title": "趋势",
                            "why_this_chart": "展示趋势",
                            "dimension": {"name": "date", "values": ["04-21", "04-22"]},
                            "metrics": [{"name": "spend", "values": [100, 120]}],
                        },
                    },
                    {
                        "type": "table",
                        "table": {
                            "title": "明细",
                            "columns": ["date", "spend"],
                            "rows": [["04-21", "100"], ["04-22", "120"]],
                        },
                    },
                ],
            },
        }
    )

    assert result.ok is False
    assert "card must not contain both chart and table blocks" in result.errors


def test_feishu_contract_requires_chart_title_and_why_this_chart():
    result = validate_and_normalize_contract(
        {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "title": "report",
                "summary": "summary",
                "blocks": [
                    {"type": "markdown", "markdown": "结论"},
                    {
                        "type": "chart",
                        "chart": {
                            "chart_type": "line",
                            "dimension": {"name": "date", "values": ["04-21", "04-22"]},
                            "metrics": [{"name": "spend", "values": [100, 120]}],
                        },
                    },
                ],
            },
        },
        channel_name="feishu",
    )

    assert result.ok is False
    assert "blocks[1].chart.title must be a non-empty string" in result.errors


def test_feishu_presentation_retry_feedback_contains_schema_and_quality_errors():
    schema_result = validate_and_normalize_contract(
        {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "title": "",
                "summary": "",
                "blocks": [
                    {
                        "type": "chart",
                        "chart": {
                            "chart_type": "line",
                            "dimension": {"name": "date", "values": ["04-21"]},
                            "metrics": [{"name": "spend", "values": [100]}],
                        },
                    }
                ],
            },
        },
        channel_name="feishu",
    )
    quality_result = validate_contract_quality(
        {
            "card_schema_version": "v1",
            "target_channel": "feishu",
            "render_mode": "card",
            "fallback_text": "fallback",
            "card_payload": {
                "title": "",
                "summary": "",
                "blocks": [
                    {
                        "type": "chart",
                        "chart": {
                            "chart_type": "line",
                            "title": "trend",
                            "why_this_chart": "show trend",
                            "dimension": {"name": "date", "values": ["04-21"]},
                            "metrics": [{"name": "spend", "values": [100]}],
                        },
                    }
                ],
            },
        }
    )

    feedback = build_feishu_presentation_retry_feedback(
        schema_result=schema_result,
        quality_result=quality_result,
    )

    assert any("card_payload.title" in item for item in feedback.schema_errors)
    assert "missing card title" in feedback.quality_errors


def test_feishu_presentation_prompt_requires_chart_table_mutual_exclusion():
    prompt = build_feishu_presentation_prompt(
        analysis_text="近14天花费呈上升趋势",
        artifacts=[],
        attachments=[],
        retry_feedback=None,
    )
    assert "Do not include both chart and table blocks in the same card." in prompt
    assert "prefer chart and omit table" in prompt
