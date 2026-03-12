"""Event platform for Ramses RF events."""

# see https://github.com/home-assistant/core/blob/dev/homeassistant/components/
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from homeassistant.components.event import EventEntity, EventEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_platform
from homeassistant.helpers.entity_platform import AddEntitiesCallback

# from homeassistant.const import ATTR_ID
# from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from .const import DOMAIN
from .coordinator import RamsesCoordinator

_LOGGER = logging.getLogger(__name__)

# adapted from Event platform for Home Assistant Backup integration.
# https://github.com/home-assistant/core/blob/dev/homeassistant/components/backup/event.py
ATTR_FAILED_REASON: Final[str] = "failed_reason"


@dataclass(frozen=True, kw_only=True, slots=True)
class RamsesListenEvent:
    """Ramses message received."""

    # manager_state: BackupManagerState = BackupManagerState.CREATE_BACKUP
    # reason: str | None
    # stage: CreateBackupStage | None
    state: RamsesEventType


class RamsesEventType(StrEnum):
    """Create ramses_cc state enum."""

    LEARN = "ramses_cc_learn"
    # TIMEOUT = "ramses_cc_timeout"
    REGEX = "ramses_cc_regex_match"


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up the event platform."""

    coordinator: RamsesCoordinator = hass.data[DOMAIN][entry.entry_id]
    platform = entity_platform.async_get_current_platform()

    @callback
    def add_devices(devices: list[RamsesEventType]) -> None:
        entities = [
            description.ramses_cc_class(coordinator, rf_device, description)
            for rf_device in devices
            for description in RAMSES_DESCRIPTIONS
            if isinstance(rf_device, description.ramses_rf_class)
            and hasattr(rf_device, description.ramses_rf_attr)
        ]
        async_add_entities(entities)

    coordinator.async_register_platform(platform, add_devices)


# async def async_setup_entry(
#     hass: HomeAssistant,
#     config_entry: ConfigEntry,
#     async_add_entities: AddConfigEntryEntitiesCallback,
# ) -> None:
#     """Event set up for Ramses RF entry."""
#     coordinator = config_entry.runtime_data
#     async_add_entities(
#         [RamsesEvent(coordinator, {}, None)],
#     )


# class RamsesEvent(RamsesEntity, EventEntity):
#     """Representation of a ramses_cc event."""
#
#     _attr_event_types = [s.value for s in RamsesEventType]
#     _unrecorded_attributes = frozenset({ATTR_FAILED_REASON})
#     coordinator: RamsesCoordinator
#
#     def __init__(self, coordinator: RamsesCoordinator) -> None:
#         """Initialize the ramses_cc event."""
#         super().__init__(coordinator)
#         self._attr_unique_id = "automatic_backup_event"
#         self._attr_translation_key = "automatic_backup_event"
#
#     @callback
#     def _handle_coordinator_update(self) -> None:
#         """Handle updated data from the coordinator."""
#         if (
#             not (data := self.coordinator.data)
#             or (event := data.last_event) is None
#             or not isinstance(event, str)
#         ):
#             return
#
#         self._trigger_event(
#             event.state,
#             {
#                 ATTR_FAILED_REASON: event.reason,
#             },
#         )
#         self.async_write_ha_state()


class RamsesEvent(EventEntity):
    """Representation of a Ramses RF event."""

    _attr_event_types = [
        "ramses_cc_regex_match",
        "ramses_cc_learn",
    ]  # simple setup, TODO use enum?

    def __init__(
        self, coordinator: RamsesCoordinator, data: dict[str, Any], event_callback: Any
    ) -> None:
        """Initialize the event.

        :param coordinator: The data update coordinator for the integration.
        :param data: Supporting data to send with the event
        """
        self._coordinator: RamsesCoordinator = coordinator
        self._type: str = data.pop("type")
        self._data = data
        self._event_callback = event_callback
        self._remove: Callable[[], None] | None = None

        super().__init__()
        _LOGGER.debug("EBR RamsesEvent init completed for %s", self._type)

    def update(
        self,
        data: dict[str, Any],
    ) -> None:
        """Update the event from async_process_msg()."""
        self._type = data.pop("type")
        self._data = data
        self._async_handle_event(self._type)

    @callback
    def _async_handle_event(self, event: str) -> None:
        """Handle the ramses event."""
        # self.hass.bus.fire(event, self._data)
        self._trigger_event(
            event,
            {
                "extra_data": self._data,
            },
        )
        self.async_write_ha_state()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        if (
            not (data := self._coordinator.data)
            or (event := data.last_event) is None
            or not isinstance(event, str)
        ):
            return

        self._trigger_event(
            event,
            {
                "extra_data": self._data,
            },
        )
        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        """Register callbacks with the coordinator and store result to allow their removal."""
        if self._coordinator.client:
            await super().async_added_to_hass()
            _LOGGER.debug("EBR RamsesEvent added_to_hass completed")
            self._remove = self._coordinator.client.add_msg_handler(
                self._event_callback
            )

    async def async_will_remove_from_hass(self) -> None:
        """Deregister callbacks with the coordinator."""
        if self._coordinator.client and self._remove is not None:
            self._remove()
        await super().async_will_remove_from_hass()


@dataclass(frozen=True, kw_only=True)
class RamsesEventEntityDescription(EventEntityDescription):
    """Class describing Ramses event entities."""

    entity_category: EntityCategory | None = EntityCategory.DIAGNOSTIC
    icon_off: str | None = None

    # integration-specific attributes
    ramses_cc_class: type[RamsesEvent] = RamsesEvent
    ramses_rf_attr: str


RAMSES_DESCRIPTIONS: tuple[RamsesEventEntityDescription, ...] = (
    RamsesEventEntityDescription(
        key="learn",
        name="learn event",
        ramses_cc_class=RamsesEvent,
        ramses_rf_attr="learn",
    ),
    RamsesEventEntityDescription(
        key="regex_match",
        name="regex match event",
        ramses_cc_class=RamsesEvent,
        ramses_rf_attr="regex_match",
    ),
)
