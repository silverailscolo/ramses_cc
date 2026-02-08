"""Helper utilities for ramses_cc."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


def ha_device_id_to_ramses_device_id(
    hass: HomeAssistant, ha_device_id: str
) -> str | None:
    """Return a RAMSES device_id (e.g. "32:153289") for a HA device registry id.

    The HA device id is the opaque string shown when using service UI targets.

    :param hass: The Home Assistant instance.
    :param ha_device_id: The Home Assistant device registry ID.
    :return: The RAMSES device ID (e.g., '01:123456') or None if not found.
    """

    if not ha_device_id:
        return None

    dev_reg = dr.async_get(hass)
    device_entry = dev_reg.async_get(ha_device_id)
    if not device_entry:
        return None

    for domain, dev_id in device_entry.identifiers:
        if domain == DOMAIN:
            return str(dev_id)

    _LOGGER.debug(
        "HA device_id %s has no %s identifier in device registry", ha_device_id, DOMAIN
    )
    return None


def ramses_device_id_to_ha_device_id(
    hass: HomeAssistant, ramses_device_id: str
) -> str | None:
    """Return a HA device registry id for a RAMSES device_id.

    :param hass: The Home Assistant instance.
    :param ramses_device_id: The RAMSES device ID (e.g., '01:123456').
    :return: The Home Assistant device registry ID or None if not found.
    """

    if not ramses_device_id:
        return None

    dev_reg = dr.async_get(hass)
    device_entry = dev_reg.async_get_device(identifiers={(DOMAIN, ramses_device_id)})
    if not device_entry:
        return None

    return device_entry.id


def fields_to_aware(dt_or_none: datetime | str | None) -> datetime | None:
    """Convert a potentially naive datetime or string to an aware datetime.

    :param dt_or_none: The datetime object, ISO string, or None to convert.
    :return: An aware datetime object or None.
    """
    if dt_or_none is None:
        return None

    # Use a local variable to help Mypy track the type conversion
    final_dt: datetime | None

    # If it's a string (common in tests or certain library states), parse it
    if isinstance(dt_or_none, str):
        final_dt = dt_util.parse_datetime(dt_or_none)
    else:
        final_dt = dt_or_none

    # Check if parsing failed or if we have a valid datetime
    if final_dt is None:
        return None

    # At this point, Mypy knows final_dt is strictly a datetime object
    if final_dt.tzinfo is not None:
        return final_dt

    # If it is naive, assume it is Local Time (Wall Clock) and make it aware
    return dt_util.as_local(final_dt)


def as_iso(val: Any) -> str:
    """Convert a datetime or string to a naive ISO string for comparison."""
    if isinstance(val, datetime):
        return val.replace(tzinfo=None).isoformat()
    return str(val)
