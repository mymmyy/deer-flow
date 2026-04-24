"""GET shortcut endpoints for fixed run presets.

These endpoints provide a stable GET API surface for frontend buttons that
need fixed model/mode settings without assembling full RunCreateRequest bodies.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from fastapi import APIRouter, Query, Request

from app.gateway.deps import get_checkpointer
from app.gateway.routers.thread_runs import RunCreateRequest
from app.gateway.services import start_run
from deerflow.runtime import serialize_channel_values

router = APIRouter(prefix="/api/runs", tags=["runs"])

# Fixed preset for button-triggered runs (keep this centralized for easy merge/review).
_FIXED_RUN_CONTEXT: dict[str, Any] = {
    "model_name": "gpt-5.2",
    "thinking_enabled": True,
    "is_plan_mode": False,
    "reasoning_effort": "medium",
    "subagent_enabled": True,
    "max_concurrent_subagents": 2,
}


@router.get("/quick/wait", response_model=dict)
async def quick_wait(
    request: Request,
    message: str = Query(..., min_length=1, description="User message"),
    thread_id: str | None = Query(default=None, description="Optional existing thread_id"),
) -> dict:
    """Run a fixed model/mode request and return the final state.

    This is a convenience wrapper around POST /api/runs/wait with a fixed
    context preset, designed for simple frontend GET calls.
    """
    resolved_thread_id = thread_id or str(uuid.uuid4())
    body = RunCreateRequest(
        assistant_id="lead_agent",
        input={"messages": [{"role": "user", "content": message}]},
        config={"configurable": {"thread_id": resolved_thread_id}},
        context=dict(_FIXED_RUN_CONTEXT),
        multitask_strategy="reject",
        stream_mode=["values"],
    )
    record = await start_run(body, resolved_thread_id, request)

    if record.task is not None:
        try:
            await record.task
        except asyncio.CancelledError:
            pass

    checkpointer = get_checkpointer(request)
    config = {"configurable": {"thread_id": resolved_thread_id}}
    checkpoint_tuple = await checkpointer.aget_tuple(config)
    if checkpoint_tuple is not None:
        checkpoint = getattr(checkpoint_tuple, "checkpoint", {}) or {}
        channel_values = checkpoint.get("channel_values", {})
        result = serialize_channel_values(channel_values)
    else:
        result = {"status": record.status.value, "error": record.error}

    # Surface key run metadata so callers can continue the same thread easily.
    result.setdefault("thread_id", resolved_thread_id)
    result.setdefault("run_id", record.run_id)
    return result

