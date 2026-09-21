"""Tests for the diagnostics platform (ramses-rf/ramses_cc issue 1214)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.ramses_cc import diagnostics
from custom_components.ramses_cc.const import (
    CONF_ADDITIONAL_PORTS,
    DOMAIN,
    SZ_PACKET_LOG,
    SZ_PACKET_LOG_PATH,
    SZ_PACKET_LOG_PREFIX,
    SZ_PORT_NAME,
    SZ_SERIAL_PORT,
)
from custom_components.ramses_cc.diagnostics import (
    async_get_config_entry_diagnostics,
)


def _entry(hass: HomeAssistant, **kwargs: Any) -> MockConfigEntry:
    """Create and register a ramses_cc MockConfigEntry."""
    entry = MockConfigEntry(domain=DOMAIN, title="RAMSES", **kwargs)
    entry.add_to_hass(hass)
    return entry


def _mock_coordinator() -> MagicMock:
    """Return a mock RamsesCoordinator with a mock Gateway client."""
    coordinator = MagicMock()
    coordinator.is_pool_enabled = False
    coordinator.get_pool_child_status.return_value = []
    coordinator.discovery_manager = None

    client = MagicMock()
    client.schema = AsyncMock(return_value={"main_tcs": "01:123456"})
    client.status = AsyncMock(return_value={"_tx_rate": None})
    client._config = AsyncMock(return_value={"known_list": {"01:123456": {}}})
    client._engine._transport.get_extra_info.return_value = None
    coordinator.client = client
    return coordinator


async def test_diagnostics_entry_only(
    hass: HomeAssistant, tmp_path: Path
) -> None:
    """Minimal dump without a coordinator (entry not loaded)."""
    # isolate the config dir: the shared testing_config dir may contain
    # a home-assistant.log left behind by an earlier test
    hass.config.config_dir = str(tmp_path)

    entry = _entry(hass, data={}, options={})

    diag = await async_get_config_entry_diagnostics(hass, entry)

    assert diag["entry"]["entry_id"] == entry.entry_id
    assert diag["entry"]["title"] == "RAMSES"
    assert diag["entry"]["data"] == {}
    assert diag["entry"]["options"] == {}
    assert diag["versions"]["integration"]
    assert diag["versions"]["ramses_rf"]
    assert diag["versions"]["ramses_tx"]
    assert diag["entities"]["total"] == 0
    assert diag["devices"]["total"] == 0
    # no coordinator: no gateway/transport/discovery sections
    assert "gateway" not in diag
    assert "transport" not in diag
    assert "discovery" not in diag
    # logs are always included; no packet log configured
    assert diag["logs"]["home_assistant_log"]["head"] == []
    assert diag["logs"]["home_assistant_log"]["tail"] == []
    assert diag["logs"]["home_assistant_log"]["errors"] == {
        "total_matches": 0,
        "blocks_omitted": 0,
        "blocks": [],
    }
    assert diag["logs"]["home_assistant_log_previous"] is None
    assert diag["logs"]["packet_log"] is None


async def test_diagnostics_redacts_options(hass: HomeAssistant) -> None:
    """Port paths, URL credentials and credential keys are redacted."""
    entry = _entry(
        hass,
        data={"password": "s3cret"},
        options={
            SZ_SERIAL_PORT: {SZ_PORT_NAME: "/dev/serial/by-id/usb-X123"},
            CONF_ADDITIONAL_PORTS: [
                "mqtt://user:pass@broker.local:1883/RAMSES/GATEWAY/18:000730",
                "/dev/ttyUSB9",
            ],
        },
    )

    diag = await async_get_config_entry_diagnostics(hass, entry)

    options = diag["entry"]["options"]
    assert options[SZ_SERIAL_PORT][SZ_PORT_NAME] == "**REDACTED**"
    assert "user:pass" not in str(options[CONF_ADDITIONAL_PORTS])
    assert "***:***@broker.local" in options[CONF_ADDITIONAL_PORTS][0]
    assert options[CONF_ADDITIONAL_PORTS][1] == "**REDACTED**"
    assert diag["entry"]["data"]["password"] == "**REDACTED**"


async def test_diagnostics_with_coordinator(hass: HomeAssistant) -> None:
    """Gateway/transport/discovery sections are collected."""
    entry = _entry(hass, data={}, options={})
    coordinator = _mock_coordinator()
    coordinator.discovery_manager = MagicMock()
    coordinator.discovery_manager.export_state.return_value = {
        "devices": {"29:176861": {"status": "new"}}
    }
    entry.runtime_data = coordinator

    diag = await async_get_config_entry_diagnostics(hass, entry)

    assert diag["gateway"]["schema"] == {"main_tcs": "01:123456"}
    assert diag["gateway"]["status"] == {"_tx_rate": None}
    assert diag["gateway"]["config"] == {"known_list": {"01:123456": {}}}
    transport = diag["transport"]
    assert transport["is_pool_enabled"] is False
    assert transport["pool_children"] == []
    assert transport["type"] == "MagicMock"
    assert diag["discovery"]["devices"]["29:176861"]["status"] == "new"


async def test_diagnostics_gateway_error_degrades(
    hass: HomeAssistant,
) -> None:
    """A failing gateway section degrades to an error marker."""
    entry = _entry(hass, data={}, options={})
    coordinator = _mock_coordinator()
    coordinator.client.schema = AsyncMock(side_effect=RuntimeError("boom"))
    # a gateway lacking a method is skipped, not an error
    del coordinator.client.status
    # a failing discovery export degrades to an error marker too
    coordinator.discovery_manager = MagicMock()
    coordinator.discovery_manager.export_state.side_effect = RuntimeError(
        "nope"
    )
    entry.runtime_data = coordinator

    diag = await async_get_config_entry_diagnostics(hass, entry)

    assert "error" in diag["gateway"]["schema"]
    assert "status" not in diag["gateway"]
    assert diag["gateway"]["config"] == {"known_list": {"01:123456": {}}}
    assert "error" in diag["discovery"]


async def test_diagnostics_log_tails(
    hass: HomeAssistant, tmp_path: Path
) -> None:
    """Log tails are filtered, redacted and capped (issue 1214)."""
    hass.config.config_dir = str(tmp_path)

    port_path = "/dev/serial/by-id/usb-SECRET123"
    mqtt_url = "mqtt://mqttuser:mqttpass@broker.local:1883/GATEWAY/18:000730"
    entry = _entry(
        hass,
        data={},
        options={
            SZ_SERIAL_PORT: {SZ_PORT_NAME: port_path},
            CONF_ADDITIONAL_PORTS: [mqtt_url],
            SZ_PACKET_LOG: {
                SZ_PACKET_LOG_PATH: str(tmp_path),
                SZ_PACKET_LOG_PREFIX: "test_packet",
            },
        },
    )
    entry.runtime_data = _mock_coordinator()

    ha_log = Path(hass.config.path("home-assistant.log"))
    ha_log.write_text(
        "2026-09-21 INFO (MainThread) [homeassistant.core] started\n"
        f"2026-09-21 WARNING (MainThread) [ramses_tx.transport.port] "
        f"PortTransport: !I timed out on {port_path}, falling back\n"
        f"2026-09-21 WARNING (MainThread) [ramses_cc] connecting {mqtt_url}\n"
        f"2026-09-21 WARNING (MainThread) [ramses_cc] auth failed: "
        f"'mqttpass' rejected, wrote log to {tmp_path}/x.log\n",
        encoding="utf-8",
    )
    packet_log = tmp_path / "test_packet.log"
    packet_log.write_text(
        "2026-09-21T21:08:01.669 060  I --- 37:153226 --:------ "
        "37:153226 12A0 021 003E07E07FFF00\n",
        encoding="utf-8",
    )

    diag = await async_get_config_entry_diagnostics(hass, entry)

    ha_lines = diag["logs"]["home_assistant_log"]["head"]
    # only ramses_* logger lines are included
    assert len(ha_lines) == 3
    assert all("ramses" in line for line in ha_lines)
    # the configured port path is scrubbed from log lines
    assert port_path not in str(ha_lines)
    assert "**REDACTED**" in ha_lines[0]
    # URL credentials are masked, even though the whole URL is configured
    assert "mqttuser" not in str(ha_lines)
    assert "mqttpass" not in str(ha_lines)
    assert "***:***@broker.local" in ha_lines[1]
    # the bare credential value and the configured log dir are scrubbed too
    assert str(tmp_path) not in ha_lines[2]
    assert "**REDACTED**" in ha_lines[2]
    # the file fits in the windows: the tail doesn't repeat the head
    assert diag["logs"]["home_assistant_log"]["tail"] == []

    # the 3 WARNING lines merge into one context block covering the
    # whole file — context lines are NOT filtered to ramses_*
    errors = diag["logs"]["home_assistant_log"]["errors"]
    assert errors["total_matches"] == 3
    assert errors["blocks_omitted"] == 0
    assert len(errors["blocks"]) == 1
    block = errors["blocks"][0]
    assert block["first_line"] == 1
    assert len(block["lines"]) == 4
    assert "homeassistant.core" in block["lines"][0]
    # credentials are scrubbed inside error blocks too
    assert "mqttpass" not in str(block)
    assert "***:***@broker.local" in block["lines"][2]

    # the configured log dir is masked in the options dump
    options = diag["entry"]["options"]
    assert options[SZ_PACKET_LOG][SZ_PACKET_LOG_PATH] == "**REDACTED**"

    packet = diag["logs"]["packet_log"]
    assert packet is not None
    # path is reported relative to the config dir, not absolute
    assert packet["path"] == "<config>/test_packet.log"
    assert packet["truncated"] is False
    assert len(packet["head"]) == 1
    assert "12A0" in packet["head"][0]
    assert packet["tail"] == []


async def test_diagnostics_log_tail_truncation(
    hass: HomeAssistant, tmp_path: Path, monkeypatch
) -> None:
    """Oversized log files are truncated to head+tail windows."""
    hass.config.config_dir = str(tmp_path)
    monkeypatch.setattr(diagnostics, "_LOG_HEAD_BYTES", 256)
    monkeypatch.setattr(diagnostics, "_LOG_TAIL_BYTES", 256)

    entry = _entry(
        hass,
        data={},
        options={
            SZ_PACKET_LOG: {
                SZ_PACKET_LOG_PATH: str(tmp_path),
                SZ_PACKET_LOG_PREFIX: "big",
            }
        },
    )
    entry.runtime_data = _mock_coordinator()

    packet_log = tmp_path / "big.log"
    packet_log.write_text(
        "".join(
            f"packet line {i:04d} xxxxxxxxxxxxxxxxxxxx\n" for i in range(50)
        ),
        encoding="utf-8",
    )
    # a log without warnings yields an empty error scan
    Path(hass.config.path("home-assistant.log")).write_text(
        "INFO (MainThread) [ramses_cc] all quiet\n", encoding="utf-8"
    )

    diag = await async_get_config_entry_diagnostics(hass, entry)

    assert diag["logs"]["home_assistant_log"]["errors"] == {
        "total_matches": 0,
        "blocks_omitted": 0,
        "blocks": [],
    }

    packet = diag["logs"]["packet_log"]
    assert packet is not None
    assert packet["truncated"] is True
    head, tail = packet["head"], packet["tail"]
    # head shows the start of the file, tail the end; no overlap
    assert head[0].startswith("packet line 0000")
    assert tail[-1].startswith("packet line 0049")
    assert set(head).isdisjoint(tail)
    # partial lines at the window edges are dropped
    assert all(line.startswith("packet line ") for line in head + tail)


async def test_diagnostics_log_head_tail_split(
    hass: HomeAssistant, tmp_path: Path, monkeypatch
) -> None:
    """Whole-file reads split into non-overlapping head/tail windows."""
    # packet log lives outside the config dir -> path shows basename
    hass.config.config_dir = str(tmp_path / "cfg")
    monkeypatch.setattr(diagnostics, "_LOG_HEAD_LINES", 3)
    monkeypatch.setattr(diagnostics, "_LOG_TAIL_LINES", 2)

    entry = _entry(
        hass,
        data={},
        options={
            SZ_PACKET_LOG: {
                SZ_PACKET_LOG_PATH: str(tmp_path),
                SZ_PACKET_LOG_PREFIX: "split",
            }
        },
    )
    entry.runtime_data = _mock_coordinator()

    (tmp_path / "split.log").write_text(
        "".join(f"line {i}\n" for i in range(10)), encoding="utf-8"
    )

    diag = await async_get_config_entry_diagnostics(hass, entry)

    packet = diag["logs"]["packet_log"]
    assert packet is not None
    assert packet["path"] == "split.log"
    # head takes the first 3, tail the last 2, middle 5 omitted
    assert packet["head"] == ["line 0", "line 1", "line 2"]
    assert packet["tail"] == ["line 8", "line 9"]
    assert packet["truncated"] is True

    # a short file is fully covered: nothing is omitted or duplicated
    (tmp_path / "split.log").write_text(
        "".join(f"line {i}\n" for i in range(5)), encoding="utf-8"
    )
    diag = await async_get_config_entry_diagnostics(hass, entry)
    packet = diag["logs"]["packet_log"]
    assert packet is not None
    assert packet["head"] == ["line 0", "line 1", "line 2"]
    assert packet["tail"] == ["line 3", "line 4"]
    assert packet["truncated"] is False


async def test_diagnostics_error_blocks(
    hass: HomeAssistant, tmp_path: Path
) -> None:
    """WARNING/ERROR lines get -10/+20 context; rotated log scanned."""
    hass.config.config_dir = str(tmp_path)

    lines = [f"log line {i:02d}" for i in range(1, 101)]
    lines[14] = "WARNING (MainThread) [homeassistant.components.mqtt] lost"
    lines[59] = "ERROR (MainThread) [ramses_tx] PortTransport blew up"
    Path(hass.config.path("home-assistant.log")).write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )
    # the rotated log holds the error from the previous run
    Path(hass.config.path("home-assistant.log.1")).write_text(
        "old line 1\nERROR (MainThread) [ramses_cc] crashed\nold line 3\n",
        encoding="utf-8",
    )

    entry = _entry(hass, data={}, options={})
    diag = await async_get_config_entry_diagnostics(hass, entry)

    errors = diag["logs"]["home_assistant_log"]["errors"]
    assert errors["total_matches"] == 2
    assert len(errors["blocks"]) == 2
    # -10/+20 context around each match
    assert errors["blocks"][0]["first_line"] == 5
    assert errors["blocks"][0]["lines"] == lines[4:35]
    assert errors["blocks"][1]["first_line"] == 50
    assert errors["blocks"][1]["lines"] == lines[49:80]
    # lines after the last block are not collected
    assert errors["blocks"][1]["lines"][-1] == "log line 80"

    previous = diag["logs"]["home_assistant_log_previous"]
    assert previous is not None
    prev_errors = previous["errors"]
    assert prev_errors["total_matches"] == 1
    assert prev_errors["blocks"][0]["first_line"] == 1
    assert any("crashed" in line for line in prev_errors["blocks"][0]["lines"])


async def test_diagnostics_redacts_edge_cases(hass: HomeAssistant) -> None:
    """Bare-string ports, empty values and unparsable URLs."""
    entry = _entry(
        hass,
        data={},
        options={
            SZ_SERIAL_PORT: "/dev/ttyUSB0",
            CONF_ADDITIONAL_PORTS: ["mqtt://user:pass@[::1", "", 123],
        },
    )

    diag = await async_get_config_entry_diagnostics(hass, entry)

    options = diag["entry"]["options"]
    assert options[SZ_SERIAL_PORT] == "**REDACTED**"
    # unparsable URL still gets its credentials masked by the fallback
    assert options[CONF_ADDITIONAL_PORTS][0] == "mqtt://***:***@[::1"
    assert options[CONF_ADDITIONAL_PORTS][1] == ""
    assert options[CONF_ADDITIONAL_PORTS][2] == 123
