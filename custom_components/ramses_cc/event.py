"""Support for Ramses RF events."""
# see https://github.com/home-assistant/core/blob/dev/homeassistant/components/

import logging
from collections.abc import Callable
from enum import StrEnum
from typing import Any  # , TYPE_CHECKING

from homeassistant.components.event import EventEntity

# from homeassistant.config_entries import ConfigEntry
# from homeassistant.const import ATTR_ID
from homeassistant.core import callback  # ,HomeAssistant

# from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
from .coordinator import RamsesCoordinator

_LOGGER = logging.getLogger(__name__)


class RamsesEventType(StrEnum):
    """Ramses RF event types."""

    SINGLE_PRESS = "single_press"
    PRESS = "press"
    RELEASE = "release"


LEGACY_EVENT_TYPES: dict[RamsesEventType, str] = {
    RamsesEventType.SINGLE_PRESS: "single",
    RamsesEventType.PRESS: "pressed",
    RamsesEventType.RELEASE: "released",
}


# async def async_setup_entry(
#     hass: HomeAssistant,
#     config_entry: ConfigEntry,
#     async_add_entities: AddConfigEntryEntitiesCallback,
# ) -> None:
#     """Set up the Ramses RF event platform."""
#     entry_data = config_entry.runtime_data
#
#     async_add_entities(
#         RamsesEventEntity(area_name, keypad, button, entry_data.client)
#         for area_name, keypad, button in entry_data.buttons
#     )


class RamsesEventEntity(EventEntity):
    _attr_event_types = ["ramses_cc_regex_match", "ramses_cc_learn"]

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
        self._type = data.pop("type")
        self._data = data
        self._async_handle_event(self._type)

    @callback
    def _async_handle_event(self, event: str) -> None:
        """Handle the ramses event."""
        # self.hass.bus.fire(event, self._data)
        self._trigger_event(event, {"extra_data": self._data})
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
