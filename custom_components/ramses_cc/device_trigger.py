"""Support for integration events"""

import logging
from typing import Any, Final

import voluptuous as vol  # type: ignore[import-untyped, unused-ignore]
from homeassistant.const import CONF_DEVICE_ID, CONF_DOMAIN, CONF_PLATFORM, CONF_TYPE
from homeassistant.core import HomeAssistant  # ,CALLBACK_TYPE
from homeassistant.helpers import (  # device_registry as dr,
    config_validation as cv,
    entity_registry as er,
)

# from homeassistant.helpers.trigger import TriggerActionType, TriggerInfo
# from homeassistant.helpers.typing import ConfigType
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

TRIGGER_TYPES: Final[set[str]] = {f"{DOMAIN}_regex_match", f"{DOMAIN}_learn"}

TRIGGER_SCHEMA: Final = cv.TRIGGER_BASE_SCHEMA.extend(
    {vol.Required(CONF_TYPE): vol.In(TRIGGER_TYPES)}, extra=vol.ALLOW_EXTRA
)


async def async_get_triggers(
    hass: HomeAssistant, device_id: str
) -> list[dict[str, Any]]:
    """Return a list of Ramses RF triggers."""

    # device_registry = dr.async_get(hass)
    # device = device_registry.async_get(device_id)
    registry = er.async_get(hass)
    triggers = []

    # Determine which triggers are supported by this device_id ...
    # see https://github.com/home-assistant/core/blob/dev/homeassistant/components/device_tracker/device_trigger.py

    for entry in er.async_entries_for_device(registry, device_id):
        if entry.domain != DOMAIN:
            continue

        _LOGGER.debug("device_trigger appending")

        triggers.append(
            {
                # Required fields of TRIGGER_BASE_SCHEMA
                CONF_PLATFORM: "device",
                CONF_DEVICE_ID: device_id,
                CONF_DOMAIN: DOMAIN,
                # Required fields of TRIGGER_SCHEMA
                CONF_TYPE: f"{DOMAIN}_regex_match",
            }
        )
        triggers.append(
            {
                # Required fields of TRIGGER_BASE_SCHEMA
                CONF_PLATFORM: "device",
                CONF_DEVICE_ID: device_id,
                CONF_DOMAIN: DOMAIN,
                # Required fields of TRIGGER_SCHEMA
                CONF_TYPE: f"{DOMAIN}_learn",
            }
        )

    return triggers
