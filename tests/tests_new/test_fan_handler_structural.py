"""Structural tests for FAN 2411 parameter handling (recipe R61).

These tests verify the structural checks that ha_sim_test recipe R61
performs by inspecting source code via ``inspect.getsource``.  They guard
against regressions of the chicken-and-egg bug that left all FAN parameter
``number`` entities unavailable after ramses_rf 0.58.3+ removed the daily
2411 discovery poll.

See:
  - ramses_cc issue 851 (FAN param entities unavailable)
  - ramses_cc issue 937 (while-True loop blocked HA startup)
"""

from __future__ import annotations

import inspect

from custom_components.ramses_cc.fan_handler import RamsesFanHandler
from custom_components.ramses_cc.number import RamsesNumberParam
from ramses_rf.devices.hvac_ventilators import HvacVentilator
from ramses_rf.state_projector import process_state_updates

# ---------------------------------------------------------------------------
# 1. ramses_rf: _handle_initialized_callback does NOT require supports_2411
# ---------------------------------------------------------------------------


def test_process_state_updates_calls_handle_initialized_callback() -> None:
    """Verify ``process_state_updates`` calls ``_handle_initialized_callback``.

    The fix for issue 851 moved the initialized-callback trigger into
    ``process_state_updates`` so it fires on the first message from/to a
    FAN device, before 2411 support is confirmed.
    """
    src = inspect.getsource(process_state_updates)
    assert "_handle_initialized_callback" in src, (
        "_handle_initialized_callback must be called in process_state_updates"
    )
    assert "HvacVentilator" in src, (
        "process_state_updates must check for HvacVentilator before firing "
        "the initialized callback"
    )


def test_handle_initialized_callback_no_supports_2411_guard() -> None:
    """Verify ``_handle_initialized_callback`` does NOT require ``supports_2411``.

    The old code guarded the callback with
    ``if self._initialized_callback is not None and self.supports_2411:``
    but ``supports_2411`` is only set by ``_handle_2411_message``, which
    only runs when a 2411 RP/I arrives — creating a chicken-and-egg
    situation.  The fix removed the ``supports_2411`` guard so the
    callback fires on any FAN message.
    """
    src = inspect.getsource(HvacVentilator._handle_initialized_callback)
    assert "and self.supports_2411" not in src, (
        "_handle_initialized_callback must NOT require supports_2411 — "
        "the guard was removed to fix issue 851"
    )


# ---------------------------------------------------------------------------
# 2. ramses_cc: _async_param_updated normalizes param IDs with lstrip("0")
# ---------------------------------------------------------------------------


def test_async_param_updated_normalizes_with_lstrip() -> None:
    """Verify ``_async_param_updated`` normalizes param IDs with ``lstrip("0")``.

    ramses_rf strips leading zeros from param IDs (e.g. ``"01"`` → ``"1"``)
    while the entity description keeps the original format.  The fix
    normalizes both sides with ``lstrip("0")`` before comparing.
    """
    src = inspect.getsource(RamsesNumberParam._async_param_updated)
    assert 'lstrip("0")' in src or "lstrip('0')" in src, (
        "_async_param_updated must normalize param IDs with lstrip('0') "
        "to handle leading-zero differences between ramses_rf and entity descriptions"
    )


# ---------------------------------------------------------------------------
# 3. ramses_cc: _start_param_polling uses async_track_time_interval
# ---------------------------------------------------------------------------


def test_start_param_polling_uses_async_track_time_interval() -> None:
    """Verify ``_start_param_polling`` uses ``async_track_time_interval``.

    The HA-idiomatic pattern (also used by the coordinator for discovery
    and state-save timers) is preferred over a manual loop.  The timer
    callback is fire-and-forget and is automatically cleaned up via
    ``entry.async_on_unload``.
    """
    assert hasattr(RamsesFanHandler, "_start_param_polling"), (
        "RamsesFanHandler must have a _start_param_polling method"
    )
    src = inspect.getsource(RamsesFanHandler._start_param_polling)
    assert "async_track_time_interval" in src, (
        "_start_param_polling must use async_track_time_interval for periodic polling"
    )


def test_start_param_polling_no_while_true_loop() -> None:
    """Verify ``_start_param_polling`` does NOT contain a ``while True`` loop.

    Issue 937: the old ``while True`` + ``asyncio.sleep`` loop blocked
    HA startup.  The fix replaced it with ``async_track_time_interval``.
    The docstring may mention ``while True`` (explaining why it was
    removed), so we strip the docstring before checking the body.
    """
    src = inspect.getsource(RamsesFanHandler._start_param_polling)

    # Strip the docstring so mentions of "while True" in comments/docs
    # don't cause false positives.
    body_lines: list[str] = []
    in_docstring = False
    for line in src.splitlines():
        stripped = line.strip()
        if stripped.startswith('"""'):
            in_docstring = not in_docstring
            if stripped.endswith('"""') and len(stripped) > 3:
                in_docstring = False
            continue
        if not in_docstring:
            body_lines.append(line)
    body = "\n".join(body_lines)

    assert "while True" not in body, (
        "_start_param_polling must NOT contain a while-True loop (issue 937) — "
        "use async_track_time_interval instead"
    )


# ---------------------------------------------------------------------------
# 4. ramses_cc: no _stop_param_polling and no _fan_param_poll_tasks
# ---------------------------------------------------------------------------


def test_no_stop_param_polling_method() -> None:
    """Verify ``RamsesFanHandler`` does NOT have a ``_stop_param_polling`` method.

    Cleanup is now automatic via ``entry.async_on_unload`` — the cancel
    callable returned by ``async_track_time_interval`` is registered there,
    so no manual stop method is needed.
    """
    assert not hasattr(RamsesFanHandler, "_stop_param_polling"), (
        "_stop_param_polling should be removed — cleanup is automatic "
        "via entry.async_on_unload"
    )


def test_no_fan_param_poll_tasks_dict() -> None:
    """Verify ``RamsesFanHandler.__init__`` does NOT create ``_fan_param_poll_tasks``.

    The old code tracked polling tasks in a dict that required manual
    cleanup.  With ``async_track_time_interval`` + ``async_on_unload``,
    no such dict is needed.
    """
    src = inspect.getsource(RamsesFanHandler.__init__)
    assert "_fan_param_poll_tasks" not in src, (
        "_fan_param_poll_tasks should be removed — cleanup is automatic "
        "via entry.async_on_unload"
    )
