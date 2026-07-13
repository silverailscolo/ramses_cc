"""Storage handler for RAMSES integration."""

from __future__ import annotations

import contextlib
import logging
import os
import time
from typing import Any, Final

import yaml  # type: ignore[import-untyped, unused-ignore]
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from .const import (
    STORAGE_KEY,
    STORAGE_VERSION,
    SZ_CLIENT_STATE,
    SZ_HVAC_SCHEMA,
    SZ_PACKETS,
    SZ_REMOTES,
    SZ_SCHEMA,
    SZ_TR_COMMANDS,
)
from .discovery import SZ_DISCOVERY

_LOGGER = logging.getLogger(__name__)

_BACKUP_KEY: Final[str] = "schema_backups"
_MAX_BACKUPS: Final[int] = 5
_BACKUP_DIR: Final[str] = "ramses_cc_backups"


class RamsesCcStore(Store[dict[str, Any]]):
    """HA Store subclass with a migration hook for ramses_cc .storage.

    Migration versions:
    - v1 → v2: commands moved from .storage[remotes] to schema _commands
      (Phase 3a).  remotes is kept as cache/fallback.
    - v2 → v3: known_list removed, fully derived from schema (Phase 4, TBD)
    """

    async def _async_migrate_func(
        self,
        old_major_version: int,
        old_minor_version: int,
        old_data: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Migrate stored data to the current version."""
        _LOGGER.debug(
            "Migrating ramses_cc storage: v%s.%s → v%s.%s",
            old_major_version,
            old_minor_version,
            self.version,
            self.minor_version,
        )
        if old_major_version < 2:
            old_data = _migrate_v1_to_v2(old_data)
        return old_data or {}


def _migrate_v1_to_v2(data: dict[str, Any] | None) -> dict[str, Any]:
    """Move remotes from .storage to schema _commands (Phase 3a).

    Only moves commands from .storage[remotes] — traits (_alias, _class,
    etc.) are additive _ keys and don't need migration. known_list[dev]
    [commands] is in config_entry.options (not .storage), so it's handled
    by the runtime merge at coordinator startup, not by this migration.
    """
    if not isinstance(data, dict):
        return {}
    client_state = data.get(SZ_CLIENT_STATE, {})
    remotes = data.get(SZ_REMOTES, {})
    schema = client_state.get(SZ_SCHEMA, {})

    if not isinstance(schema, dict) or not isinstance(remotes, dict):
        return data

    migrated = 0
    for device_id, commands in remotes.items():
        if not commands or not isinstance(commands, dict):
            continue
        entry = schema.get(device_id)
        if not isinstance(entry, dict):
            entry = {}
            schema[device_id] = entry
        if SZ_TR_COMMANDS not in entry:
            entry[SZ_TR_COMMANDS] = dict(commands)
            migrated += 1

    if migrated:
        _LOGGER.info(
            "Storage migration v1 → v2: moved _commands for %d device(s) "
            "from remotes to schema. remotes kept as cache.",
            migrated,
        )
        client_state[SZ_SCHEMA] = schema
        data[SZ_CLIENT_STATE] = client_state

    return data


class RamsesStore:
    """Class to handle persistence of RAMSES configuration and state."""

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the storage helper."""
        self._hass = hass
        self._store = RamsesCcStore(hass, STORAGE_VERSION, STORAGE_KEY)

    async def async_load(self) -> dict[str, Any]:
        """Load the data from the persistent storage.

        :return: The stored data or an empty dictionary if no data exists
        """
        return await self._store.async_load() or {}

    async def async_save(
        self,
        schema: dict[str, Any],
        packets: dict[str, dict[str, Any] | str],
        remotes: dict[str, Any],
        discovery: dict[str, Any] | None = None,
        hvac_schema: dict[str, Any] | None = None,
    ) -> None:
        """Save the current state to persistent storage.

        If ``discovery`` is None, any existing discovery state is preserved
        (not overwritten) — this prevents a new coordinator from wiping the
        discovery state during reload before the scan engine has started.

        If ``hvac_schema`` is None, any existing HVAC schema is preserved.

        :param schema: The current device schema
        :param packets: The cached packet log (supports legacy strings and JSON DTOs)
        :param remotes: The known remotes and their commands
        :param discovery: The discovery scan state (metadata + engine state)
        :param hvac_schema: HVAC-only schema entries (load_fan stub workaround)
        """
        data: dict[str, Any] = {
            SZ_CLIENT_STATE: {SZ_SCHEMA: schema, SZ_PACKETS: packets},
            SZ_REMOTES: remotes,
        }

        if discovery is not None:
            data[SZ_DISCOVERY] = discovery
        else:
            # Preserve existing discovery state if we don't have new data
            existing = await self._store.async_load()
            if existing and SZ_DISCOVERY in existing:
                data[SZ_DISCOVERY] = existing[SZ_DISCOVERY]

        if hvac_schema is not None:
            data[SZ_HVAC_SCHEMA] = hvac_schema
        else:
            # Preserve existing HVAC schema if we don't have new data
            existing = await self._store.async_load() or {}
            if SZ_HVAC_SCHEMA in existing:
                data[SZ_HVAC_SCHEMA] = existing[SZ_HVAC_SCHEMA]

        # Preserve existing backups (in .storage)
        existing = await self._store.async_load() or {}
        if _BACKUP_KEY in existing:
            data[_BACKUP_KEY] = existing[_BACKUP_KEY]

        await self._store.async_save(data)

    async def async_save_backup(
        self,
        schema: dict[str, Any],
        known_list: dict[str, Any],
        *,
        reason: str = "migration",
    ) -> str | None:
        """Save a backup of schema + known_list as a YAML file.

        Writes a human-readable YAML file to ``<config_dir>/ramses_cc_backups/``
        so users can open it, inspect it, and copy/paste values back into
        the schema editor if a migration goes wrong.

        Also keeps a pointer in .storage (``schema_backups`` key) with the
        file path and timestamp for the restore service to find them.

        :param schema: The schema dict before migration.
        :param known_list: The known_list dict before migration.
        :param reason: Short label for the backup filename (e.g. "migration",
            "phase2", "class_update").
        :return: The path to the backup file, or None on failure.
        """
        timestamp = time.time()
        timestamp_str = time.strftime("%Y%m%d_%H%M%S", time.localtime(timestamp))

        # Build the backup content
        backup_data = {
            "timestamp": timestamp_str,
            "reason": reason,
            "schema": schema,
            "known_list": known_list,
        }

        # Write to <config_dir>/ramses_cc_backups/
        backup_dir = self._hass.config.path(_BACKUP_DIR)
        filename = f"backup_{timestamp_str}_{reason}.yaml"
        filepath = os.path.join(backup_dir, filename)

        try:
            # Create directory if it doesn't exist (run in executor)
            await self._hass.async_add_executor_job(_ensure_backup_dir, backup_dir)
            # Write the YAML file (run in executor)
            await self._hass.async_add_executor_job(
                _write_yaml_file, filepath, backup_data
            )
        except OSError as err:
            _LOGGER.error("Failed to write backup file %s: %s", filepath, err)
            return None

        _LOGGER.info("Saved schema backup to %s (reason: %s)", filepath, reason)

        # Also track in .storage for the restore service
        existing = await self._store.async_load() or {}
        backups: list[dict[str, Any]] = existing.get(_BACKUP_KEY, [])
        backups.append(
            {
                "timestamp": timestamp,
                "reason": reason,
                "filepath": filepath,
                "filename": filename,
            }
        )
        # Trim to max backups (keep the most recent)
        if len(backups) > _MAX_BACKUPS:
            # Remove oldest backup files that are no longer tracked
            removed = backups[:-_MAX_BACKUPS]
            for entry in removed:
                old_path = entry.get("filepath")
                if old_path:
                    await self._hass.async_add_executor_job(_safe_remove, old_path)
            backups = backups[-_MAX_BACKUPS:]

        data = existing.copy()
        data[_BACKUP_KEY] = backups
        await self._store.async_save(data)

        return filepath

    async def async_load_backups(self) -> list[dict[str, Any]]:
        """Load the backup index from .storage.

        :return: A list of backup metadata dicts, each with timestamp,
            reason, filepath, filename.
        """
        existing = await self._store.async_load() or {}
        return existing.get(_BACKUP_KEY, [])

    async def async_load_backup_file(self, filepath: str) -> dict[str, Any] | None:
        """Load a specific backup YAML file.

        :param filepath: Path to the backup YAML file.
        :return: The backup dict with schema + known_list, or None on failure.
        """
        try:
            return await self._hass.async_add_executor_job(_read_yaml_file, filepath)
        except (OSError, yaml.YAMLError) as err:
            _LOGGER.error("Failed to read backup file %s: %s", filepath, err)
            return None


def _ensure_backup_dir(backup_dir: str) -> None:
    """Create the backup directory if it doesn't exist."""
    os.makedirs(backup_dir, exist_ok=True)


def _write_yaml_file(filepath: str, data: dict[str, Any]) -> None:
    """Write a YAML file with a header comment."""
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(
            f"# ramses_cc schema backup\n"
            f"# timestamp: {data['timestamp']}\n"
            f"# reason: {data['reason']}\n"
            f"# This file was created automatically before a migration.\n"
            f"# You can copy/paste values from here back into the schema editor.\n\n"
        )
        yaml.dump(
            {
                "schema": data["schema"],
                "known_list": data["known_list"],
            },
            f,
            default_flow_style=False,
            sort_keys=True,
            allow_unicode=True,
        )


def _read_yaml_file(filepath: str) -> dict[str, Any]:
    """Read a YAML file."""
    with open(filepath, encoding="utf-8") as f:
        return yaml.load(f, Loader=yaml.SafeLoader)


def _safe_remove(filepath: str) -> None:
    """Remove a file, ignoring errors."""
    with contextlib.suppress(OSError):
        os.remove(filepath)
