from dataclasses import dataclass
from typing import Any
from app.channels.manager import (
    ChannelManager,
    _derive_stage_from_event,
    _extract_tool_progress_from_payload,
    _extract_tool_result_events_from_payload,
)
from app.channels.message_bus import InboundMessage, MessageBus
from app.channels.store import ChannelStore


@dataclass
class _Chunk:
    event: str
    data: Any


class _FakeRuns:
    def __init__(self, chunks):
        self._chunks = chunks

    async def stream(self, *args, **kwargs):
        for chunk in self._chunks:
            yield chunk


class _FakeClient:
    def __init__(self, chunks):
        self.runs = _FakeRuns(chunks)


def _run(coro):
    import asyncio

    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_manager_streaming_publishes_progress_metadata():
    async def go():
        bus = MessageBus()
        store = ChannelStore()
        manager = ChannelManager(bus=bus, store=store)

        chunks = [
            _Chunk(
                event="messages-tuple",
                data=(
                    {
                        "type": "ai",
                        "content": "",
                        "tool_calls": [
                            {
                                "name": "web_search",
                                "args": {"q": "花费趋势"},
                            }
                        ],
                    },
                    {"id": "m1"},
                ),
            ),
            _Chunk(
                event="messages-tuple",
                data=(
                    {
                        "type": "ai",
                        "content": "这是阶段性输出",
                    },
                    {"id": "m2"},
                ),
            ),
            _Chunk(
                event="values",
                data={
                    "messages": [
                        {"type": "human", "content": "hi"},
                        {"type": "ai", "content": "最终结果"},
                    ]
                },
            ),
        ]

        client = _FakeClient(chunks)
        msg = InboundMessage(channel_name="feishu", chat_id="chat_1", user_id="user_1", text="hi", thread_ts="msg_1")

        outbox = []
        original_publish = bus.publish_outbound

        async def capture(outbound):
            outbox.append(outbound)
            await original_publish(outbound)

        bus.publish_outbound = capture  # type: ignore[assignment]

        await manager._handle_streaming_chat(
            client=client,
            msg=msg,
            thread_id="thread_1",
            assistant_id="lead_agent",
            run_config={},
            run_context={},
        )

        assert outbox
        non_final = [item for item in outbox if not item.is_final]
        assert non_final
        has_progress = any(
            isinstance(item.metadata, dict)
            and isinstance(item.metadata.get("progress_events"), list)
            and item.metadata.get("status_stage")
            for item in non_final
        )
        assert has_progress

    _run(go())


def test_extract_tool_progress_uses_chart_stage_for_chart_tool():
    payload = {
        "tool_calls": [
            {"name": "build_chart_spec", "args": {"x": "date", "y": "cost"}},
            {"name": "read_file", "args": {"path": "a.md"}},
        ]
    }
    events = _extract_tool_progress_from_payload(payload)
    assert events[0]["stage"] == "生成图表/表格"
    assert events[1]["stage"] == "调用工具中"


def test_derive_stage_keeps_previous_retrieval_stage_when_no_text():
    stage = _derive_stage_from_event(
        "messages-tuple",
        {"type": "ai", "content": ""},
        latest_text="",
        previous_stage="检索/读取资料",
    )
    assert stage == "检索/读取资料"


def test_manager_streaming_retries_on_timeout_and_reports_retry_status():
    async def go():
        bus = MessageBus()
        store = ChannelStore()
        manager = ChannelManager(bus=bus, store=store)

        class _TimeoutRuns:
            async def stream(self, *args, **kwargs):
                raise TimeoutError("stream timeout")
                yield  # pragma: no cover

        class _TimeoutClient:
            runs = _TimeoutRuns()

        outbox = []
        original_publish = bus.publish_outbound

        async def capture(outbound):
            outbox.append(outbound)
            await original_publish(outbound)

        bus.publish_outbound = capture  # type: ignore[assignment]

        msg = InboundMessage(channel_name="feishu", chat_id="chat_1", user_id="user_1", text="hi", thread_ts="msg_1")

        manager._stream_max_retries = 1
        manager._stream_retry_base_delay_seconds = 0.0
        manager._stream_attempt_timeout_seconds = 1.0
        await manager._handle_streaming_chat(
            client=_TimeoutClient(),
            msg=msg,
            thread_id="thread_1",
            assistant_id="lead_agent",
            run_config={},
            run_context={},
        )

        non_final = [item for item in outbox if not item.is_final and isinstance(item.metadata, dict)]
        retry_events = [
            item
            for item in non_final
            if item.metadata.get("status_stage") == "重试中"
            and any("重试" in str(event.get("detail", "")) for event in item.metadata.get("progress_events", []))
        ]
        assert retry_events
        final = [item for item in outbox if item.is_final][-1]
        assert isinstance(final.metadata, dict)
        assert final.metadata.get("status_stage") == "发送结果"
        assert "An error occurred" in final.text

    _run(go())


def test_manager_streaming_config_bounds_are_clamped():
    bus = MessageBus()
    store = ChannelStore()
    manager = ChannelManager(
        bus=bus,
        store=store,
        streaming_config={
            "attempt_timeout_seconds": 0,
            "max_retries": 999,
            "retry_base_delay_seconds": -3,
        },
    )
    assert manager._stream_attempt_timeout_seconds == 5.0
    assert manager._stream_max_retries == 8
    assert manager._stream_retry_base_delay_seconds == 0.0


def test_extract_tool_result_events_success_and_failure():
    success_payload = {
        "type": "tool",
        "name": "read_file",
        "content": "loaded report.md",
    }
    failure_payload = {
        "type": "tool",
        "name": "web_search",
        "status": "failed",
        "content": "timeout",
    }

    success_events = _extract_tool_result_events_from_payload(success_payload)
    failure_events = _extract_tool_result_events_from_payload(failure_payload)

    assert success_events and success_events[0]["stage"] == "工具完成"
    assert "read_file" in success_events[0]["detail"]
    assert failure_events and failure_events[0]["stage"] == "工具失败"
    assert "web_search" in failure_events[0]["detail"]


def test_manager_streaming_publishes_tool_result_status():
    async def go():
        bus = MessageBus()
        store = ChannelStore()
        manager = ChannelManager(bus=bus, store=store)

        chunks = [
            _Chunk(
                event="messages-tuple",
                data=(
                    {
                        "type": "ai",
                        "content": "",
                        "tool_calls": [{"name": "read_file", "args": {"path": "report.md"}}],
                    },
                    {"id": "m1"},
                ),
            ),
            _Chunk(
                event="messages-tuple",
                data=(
                    {
                        "type": "tool",
                        "name": "read_file",
                        "content": "ok",
                    },
                    {"id": "t1"},
                ),
            ),
            _Chunk(
                event="values",
                data={
                    "messages": [
                        {"type": "human", "content": "hi"},
                        {"type": "ai", "content": "done"},
                    ]
                },
            ),
        ]

        client = _FakeClient(chunks)
        msg = InboundMessage(channel_name="feishu", chat_id="chat_1", user_id="user_1", text="hi", thread_ts="msg_1")

        outbox = []
        original_publish = bus.publish_outbound

        async def capture(outbound):
            outbox.append(outbound)
            await original_publish(outbound)

        bus.publish_outbound = capture  # type: ignore[assignment]

        await manager._handle_streaming_chat(
            client=client,
            msg=msg,
            thread_id="thread_1",
            assistant_id="lead_agent",
            run_config={},
            run_context={},
        )

        non_final = [item for item in outbox if not item.is_final and isinstance(item.metadata, dict)]
        assert any(item.metadata.get("status_stage") == "工具完成" for item in non_final)
        assert any(
            any(event.get("stage") == "工具完成" and "read_file" in str(event.get("detail", "")) for event in item.metadata.get("progress_events", []))
            for item in non_final
        )

    _run(go())
