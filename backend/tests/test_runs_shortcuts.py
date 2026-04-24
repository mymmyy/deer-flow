import asyncio
from unittest.mock import AsyncMock, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.gateway.routers import runs_shortcuts


def test_quick_wait_uses_fixed_context_and_returns_thread_and_run_ids():
    app = FastAPI()
    app.include_router(runs_shortcuts.router)

    # Fake app state dependencies expected by get_checkpointer/start_run helpers.
    app.state.checkpointer = MagicMock()
    app.state.checkpointer.aget_tuple = AsyncMock(return_value=None)
    app.state.stream_bridge = MagicMock()
    app.state.run_manager = MagicMock()
    app.state.store = MagicMock()

    fake_record = MagicMock()
    fake_record.run_id = "run_test"
    fake_record.status.value = "success"
    fake_record.error = None
    done_task = asyncio.Future()
    done_task.set_result(None)
    fake_record.task = done_task

    # Patch start_run in shortcut module only (minimal surface).
    original_start_run = runs_shortcuts.start_run
    runs_shortcuts.start_run = AsyncMock(return_value=fake_record)
    try:
        with TestClient(app) as client:
            resp = client.get("/api/runs/quick/wait", params={"message": "hello"})
            assert resp.status_code == 200
            data = resp.json()
            assert data["run_id"] == "run_test"
            assert "thread_id" in data

            called_body = runs_shortcuts.start_run.await_args.args[0]
            assert called_body.context["model_name"] == "gpt-5.2"
            assert called_body.context["is_plan_mode"] is False
            assert called_body.context["thinking_enabled"] is True
    finally:
        runs_shortcuts.start_run = original_start_run
