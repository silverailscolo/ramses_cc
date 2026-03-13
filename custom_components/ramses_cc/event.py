"""Event platform for Ramses RF events."""

# see https://github.com/home-assistant/core/blob/dev/homeassistant/components/
from __future__ import annotations

import logging
from collections.abc import Callable
from re import Pattern

# from dataclasses import dataclass
# from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

from homeassistant.components.event import EventEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .const import DOMAIN, SIGNAL_UPDATE
from .coordinator import RamsesCoordinator

if TYPE_CHECKING:
    from ramses_tx.message import Message

_LOGGER = logging.getLogger(__name__)

# adapted from Event platform for Home Assistant Backup integration.
# https://github.com/home-assistant/core/blob/dev/homeassistant/components/backup/event.py
ATTR_FAILED_REASON: Final[str] = "failed_reason"


# @dataclass(frozen=True, kw_only=True, slots=True)
# class RamsesListenEvent:
#     """Ramses message received."""
#
#     # manager_state: BackupManagerState = BackupManagerState.CREATE_BACKUP
#     # reason: str | None
#     # stage: CreateBackupStage | None
#     state: RamsesEventType
#
#
# class RamsesEventType(StrEnum):
#     """Create ramses_cc state enum."""
#
#     LEARN = "ramses_cc_learn"
#     REGEX = "ramses_cc_regex_match"


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Event set up for Ramses RF entry."""

    coordinator = hass.data[DOMAIN][config_entry.entry_id]
    async_add_entities(
        [
            RamsesLearnEvent(
                coordinator,
                hass,
                data={"type": f"{DOMAIN}_learn"},
            ),
            RamsesRegexEvent(
                coordinator,
                hass,
                data={"type": f"{DOMAIN}_regex_match"},
            ),
        ]
    )


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

    def __init__(
        self,
        coordinator: RamsesCoordinator,
        hass: HomeAssistant,
        data: dict[str, Any],
        event_callback: Any,
    ) -> None:
        """Initialize the event."""
        self._coordinator: RamsesCoordinator = coordinator
        self._hass = hass
        self._type: str = data["type"]  # data.pop("type")
        self._data = data
        self._event_callback = event_callback
        self._remove: Callable[[], None] | None = None
        super().__init__()
        _LOGGER.debug("EBR RamsesEvent init completed for %s", self._type)

    def update_data(self, data: dict[str, Any]) -> None:
        """Update the event from async_process_msg()."""
        self._type = data["type"]  # data.pop("type")
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

    # @callback
    # def _handle_coordinator_update(self) -> None:
    #     """Handle updated data from the coordinator."""
    #     if (
    #         not (data := self._coordinator.data)
    #         or (event := data.last_event) is None
    #         or not isinstance(event, str)
    #     ):
    #         return
    #
    #     self._trigger_event(
    #         event,
    #         {
    #             "extra_data": self._data,
    #         },
    #     )
    #     self.async_write_ha_state()

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


class RamsesLearnEvent(RamsesEvent):
    """Representation of a Ramses RF Learn event."""

    _attr_event_types = [
        "ramses_cc_learn",
    ]

    def __init__(
        self,
        coordinator: RamsesCoordinator,
        hass: HomeAssistant,
        data: dict[str, Any],
    ) -> None:
        """Initialize the event.

        :param coordinator: The data update coordinator for the integration.
        :param hass: The Home Assistant instance.
        :param data: Supporting data to send with the event
        """

        @callback
        def async_process_msg(msg: Message, *args: Any, **kwargs: Any) -> None:
            """Process a message from the event bus and pass it on."""

            async_dispatcher_send(self._hass, f"{SIGNAL_UPDATE}_{msg.src.id}")
            if msg.dst and msg.dst.id != msg.src.id:
                async_dispatcher_send(self._hass, f"{SIGNAL_UPDATE}_{msg.dst.id}")

            if (
                coordinator.learn_device_id
                and coordinator.learn_device_id == msg.src.id
            ):
                event_data = {
                    "type": f"{DOMAIN}_learn",
                    "src": msg.src.id,
                    "code": msg.code,
                    "packet": str(msg._pkt),
                }
                # TODO: change to }_event and read that type in coordinator.learn_device_id
                super().update_data(event_data)
                # was: hass.bus.async_fire(f"{DOMAIN}_learn", event_data)

        super().__init__(coordinator, hass, data, event_callback=async_process_msg)

        self._attr_unique_id = "learn_event"
        self._attr_translation_key = "ramses_cc_learn_event"


class RamsesRegexEvent(RamsesEvent):
    """Representation of a Ramses RF Learn event."""

    _attr_event_types = [
        "ramses_cc_regex_match",
    ]

    def __init__(
        self,
        coordinator: RamsesCoordinator,
        hass: HomeAssistant,
        data: dict[str, Any],
        regex: Pattern[str] | None = None,
    ) -> None:
        """Initialize the event.

        :param coordinator: The data update coordinator for the integration.
        :param hass: The Home Assistant instance.
        :param data: Supporting data to send with the event
        :param regex: The regular expression to match against
        """
        self.regex = regex

        @callback
        def async_process_msg(msg: Message, *args: Any, **kwargs: Any) -> None:
            """Process a message from the event bus and pass it on."""

            async_dispatcher_send(self._hass, f"{SIGNAL_UPDATE}_{msg.src.id}")
            if msg.dst and msg.dst.id != msg.src.id:
                async_dispatcher_send(self._hass, f"{SIGNAL_UPDATE}_{msg.dst.id}")

            # filter msg by advanced_config regex, fire an event if a match
            if regex and regex.search(f"{msg!r}"):
                event_data = {
                    "type": f"{DOMAIN}_regex_match",
                    "device_id": msg.src.id,
                    "dtm": msg.dtm.isoformat(),
                    "src": msg.src.id,
                    "dst": msg.dst.id,
                    "verb": msg.verb,
                    "code": msg.code,
                    "payload": msg.payload,
                    "packet": str(msg._pkt),
                }
                super().update_data(event_data)
                # was _cc: hass.bus.async_fire(f"{DOMAIN}_event", event_data)

        super().__init__(coordinator, hass, data, event_callback=async_process_msg)

        self._attr_unique_id = "regex_event"
        self._attr_translation_key = "ramses_cc_regex_event"
