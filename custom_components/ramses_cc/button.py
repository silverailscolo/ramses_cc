"""Support for RAMSES RF button entities.

Adapted from number.py

Buttons are stateless momentary entities that trigger an action when
pressed. This module provides:
- gateway-level buttons that invoke the integration's own
  domain-wide services (see ./services.yaml), and
- per-FAN buttons that reset the filter counter (W 10D0) via the
  ``reset_filter_counter`` entity service (see ./remote.py).

.. rubric:: Module Functions

.. py:function:: async_setup_entry(...)

    Set up the RAMSES button platform from a config entry. Creates
    gateway-level service buttons immediately, and registers a
    device-discovery callback with the coordinator so that per-FAN
    filter-reset buttons are created as soon as both the FAN device
    and its climate entity are available.

.. rubric:: Class Structure

.. code-block:: text

    RamsesButtonBase (RamsesEntity, ButtonEntity)
    └── RamsesButtonEntityDescription (
            RamsesEntityDescription, ButtonEntityDescription
        )
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from homeassistant.components.button import (
    ButtonEntity,
    ButtonEntityDescription,
)
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import (
    device_registry as dr,
    entity_platform,
    entity_registry as er,
)
from homeassistant.helpers.entity_platform import (
    AddEntitiesCallback,
    EntityPlatform,
)

from ramses_rf.devices import HgiGateway
from ramses_rf.entity import Entity as RamsesRFEntity

from .const import DOMAIN, SVC_DISCOVER_KNOWN_DEVICES
from .entity import RamsesEntity, RamsesEntityDescription
from .helpers import device_slug, normalize_device_id
from .schemas import (
    SVC_FORCE_UPDATE,
    SVC_RESET_FILTER,
    SVC_SYNC_TOPOLOGY,
)
from .typing import RamsesConfigEntry

if TYPE_CHECKING:
    from .coordinator import RamsesCoordinator

_LOGGER = logging.getLogger(__name__)

# entity service: reset the filter counter of a FAN (W 10D0)


@dataclass(frozen=True, kw_only=True)
class RamsesButtonEntityDescription(
    RamsesEntityDescription, ButtonEntityDescription
):
    """Class describing Ramses button entities."""

    service: str | None = None  # ramses_cc service to call when pressed
    service_data: dict[str, str] | None = None
    target: dict[str, Any] | None = None
    entity_category: EntityCategory | None = EntityCategory.DIAGNOSTIC
    ramses_cc_extra_attributes: dict[str, str] | None = None


class RamsesButtonBase(RamsesEntity, ButtonEntity):
    """Base for any RAMSES II-compatible button entity."""

    entity_description: RamsesButtonEntityDescription

    _attr_has_entity_name = True

    async def async_press(self) -> None:
        """Handle the button press.

        Calls the ramses_cc service named in the entity description,
        passing any configured service data (e.g. the entity_id of the
        FAN climate entity for the filter reset).
        """
        service = self.entity_description.service
        if not service:
            _LOGGER.warning(
                "Button %s has no service configured",
                self.entity_id,
            )
            return

        _LOGGER.info(
            "Button %s calls service %s with data %s, target %s",
            self.entity_id,
            service,
            self.entity_description.service_data,
            self.entity_description.target,
        )
        await self.hass.services.async_call(
            DOMAIN,
            service,
            service_data=self.entity_description.service_data or {},
            target=self.entity_description.target or {},
            blocking=True,
        )

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return target attribute.

        :return: Dictionary of device, target.
        :rtype: dict[str, Any]
        """
        target = self.entity_description.target

        return super().extra_state_attributes | {
            "target": target,
        }


async def async_setup_entry(
    hass: HomeAssistant,
    entry: RamsesConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the RAMSES button platform from a config entry.

    Creates one gateway-level button per domain-wide service action,
    plus one filter-reset button per FAN (HVAC) device. Because the
    button platform may be set up before the HGIs are loaded, or the
    climate platform has registered the FAN entities, a discovery
    callback is registered with the coordinator: as devices (re)appear,
    any missing filter-reset buttons are created once their climate
    entity exists.

    :param hass: The Home Assistant instance
    :type hass: ~homeassistant.core.HomeAssistant
    :param entry: The config entry used to set up the platform
    :type entry: ~homeassistant.config_entries.ConfigEntry
    :param async_add_entities: Async function to add entities to the
        platform
    :type async_add_entities:
        ~homeassistant.helpers.entity_platform.AddEntitiesCallback
    :return: None
    :rtype: None
    """
    coordinator: RamsesCoordinator = entry.runtime_data
    platform: EntityPlatform = entity_platform.async_get_current_platform()
    # ent_reg = er.async_get(hass)

    # unique_ids of buttons already created (or scheduled)
    known_buttons: set[str] = set()
    buttons: list[RamsesButtonBase] = []

    _LOGGER.debug("Setting up button platform")

    def _create_hgi_buttons(
        coordinator: RamsesCoordinator, hgi: RamsesRFEntity
    ) -> list[RamsesButtonBase]:
        """Create buttons for a HGI devices.

        Prevents duplicates.

        :param devices: HGI devices to process
        :return: Newly created button entity (may be None)
        """
        # created_button_entities = coordinator._button_entities_created
        new_buttons: list[RamsesButtonBase] = []

        if not isinstance(hgi, HgiGateway):
            return []

        if hgi.id is not None:
            # Normalize device ID once at the start
            device_id = normalize_device_id(hgi.id)
            _LOGGER.debug("Adding HGI Buttons for %s", device_id)

            for description in (
                RamsesButtonEntityDescription(
                    key="force_update",
                    translation_key="force_update",
                    icon="mdi:refresh",
                    service=SVC_FORCE_UPDATE,
                ),
                RamsesButtonEntityDescription(
                    key="sync_topology",
                    translation_key="sync_topology",
                    icon="mdi:lan-connect",
                    service=SVC_SYNC_TOPOLOGY,
                ),
                RamsesButtonEntityDescription(
                    key="discover_known_devices",
                    translation_key="discover_known_devices",
                    icon="mdi:magnify",
                    service=SVC_DISCOVER_KNOWN_DEVICES,
                ),
            ):
                new_unique_id = f"{device_id}-{description.key}"
                if (
                    # new_unique_id in created_button_entities
                    # or
                    new_unique_id in known_buttons
                ):
                    _LOGGER.debug(
                        "Button entity %s already loaded, skipping duplicate",
                        new_unique_id,
                    )
                    continue
                button = RamsesButtonBase(coordinator, hgi, description)
                button.name = description.key
                button._attr_unique_id = new_unique_id
                known_buttons.add(new_unique_id)
                new_buttons.append(button)
        else:
            _LOGGER.warning(
                "No gateway device found yet; skipping gateway service buttons for now"
            )

        return new_buttons

    def _create_fan_buttons(
        coordinator: RamsesCoordinator, fan: RamsesRFEntity
    ) -> list[RamsesButtonBase]:
        """Create filter-reset buttons for any new FAN devices.

        :param devices: FAN devices to process
        :return: Newly created button entities (may be empty)
        """
        new_buttons: list[RamsesButtonBase] = []

        if getattr(fan, "_SLUG", None) != "FAN":  # no button on REM
            return []

        _LOGGER.debug("Adding FAN Button for %s", fan.id)
        # Normalize device ID once at the start
        device_id = normalize_device_id(fan.id)

        bound_rem: str | None = fan.get_bound_rem() or None
        _LOGGER.debug("bound_rem: %s", bound_rem)
        # bound_rem: 29:123160

        if bound_rem is None:
            _LOGGER.debug(
                "No bound rem (%s) defined for FAN %s; will retry when"
                " devices are (re)discovered",
                bound_rem,
                fan.id,
            )
            return []

        # look up by uid
        remote_entity_id = _fan_remote_entity_id(hass, fan.id)

        if remote_entity_id is None:
            _LOGGER.debug(
                "No remote entity yet for FAN REM %s; will retry when"
                " devices are (re)discovered",
                fan.id,
            )
            return []

        _LOGGER.debug(
            "Preparing Filter Counter Reset button entity, targeting REM %s on FAN %s",
            remote_entity_id,
            fan.id,
        )

        description = RamsesButtonEntityDescription(
            key="reset_filter_counter",
            translation_key="reset_filter_counter",
            icon="mdi:restart-alert",
            service=SVC_RESET_FILTER,
            target={"entity_id": {remote_entity_id}},
            ramses_cc_extra_attributes={"target": remote_entity_id},
        )
        new_unique_id = f"{device_id}-{description.key}"
        if new_unique_id in known_buttons:
            return []  # already created/scheduled during this setup

        button = RamsesButtonBase(coordinator, fan, description)
        button.name = description.key
        button._attr_unique_id = new_unique_id
        button._attr_name = "Reset Filter Counter"
        button._attr_device_info = dr.DeviceInfo(
            identifiers={(DOMAIN, remote_entity_id)},
            name="Reset Filter Counter",
        )

        known_buttons.add(new_unique_id)
        _LOGGER.debug("Updated known_buttons: %s", known_buttons)
        new_buttons.append(button)
        return new_buttons

    def _fan_remote_entity_id(hass: HomeAssistant, fan_id: str) -> str | None:
        """Return the remote entity_id registered on a FAN device.

        The ``reset_filter_counter`` entity service accepts a FAN REM
        entity_id as its target (the handler resolves the bound REM itself).

        :param hass: The Home Assistant instance
        :param fan_id: The device id of the FAN
        :return: The climate entity_id, or None if not yet registered
        """
        ent_reg = er.async_get(hass)
        entities = er.async_entries_for_config_entry(
            ent_reg, coordinator.entry.entry_id
        )

        for entity in entities:
            if entity.domain == "remote":
                if entity.unique_id == str(fan_id):
                    _LOGGER.debug("Found matching remote entity ID %s", fan_id)
                    return entity.entity_id
        return None

    @callback
    def add_devices(
        devices: RamsesRFEntity
        | RamsesButtonBase
        | list[RamsesRFEntity | RamsesButtonBase],
    ) -> None:
        """Add button entities for newly discovered devices.

        The coordinator invokes this
        callback whenever devices are discovered or re-discovered.

        :param devices: Device(s) to process
        :return: None
        """
        # gateway_id: str | None = None

        device_list = devices if isinstance(devices, Sequence) else [devices]

        _LOGGER.debug("Processing %d items", len(device_list))
        if not device_list:
            return

        # If we received entities directly (not devices), just add them
        pending_entities = coordinator._button_entities_pending
        loaded_entities = coordinator._button_entities_loaded

        if all(isinstance(d, RamsesButtonBase) for d in device_list):
            _LOGGER.debug("Adding %d entities directly", len(device_list))
            entities_to_add = []
            for entity in device_list:
                if not isinstance(entity, RamsesButtonBase):
                    continue
                entity_id = entity.entity_id
                unique_id = entity.unique_id

                # Type guard string to satisfy Pyright's Set[str] requirement
                if not isinstance(unique_id, str):
                    continue

                # Check if entity already exists in platform by entity_id
                if (
                    hasattr(platform, "entities")
                    and entity_id in platform.entities
                ):
                    _LOGGER.debug(
                        "Entity %s already loaded in platform, skipping",
                        entity_id,
                    )
                    continue

                if unique_id in pending_entities:
                    _LOGGER.debug(
                        "Entity unique_id %s already scheduled, skipping",
                        unique_id,
                    )
                    continue

                pending_entities.add(unique_id)
                entities_to_add.append(entity)

            if entities_to_add:
                _LOGGER.debug(
                    "Adding %d new entities directly", len(entities_to_add)
                )
                async_add_entities(entities_to_add)
                loaded_entities.update(
                    e.unique_id
                    for e in entities_to_add
                    if isinstance(e.unique_id, str)
                )
            return

        # Otherwise, process as devices and create entities
        for _device in device_list:
            if not isinstance(_device, RamsesRFEntity):
                _LOGGER.debug("Skipping non-device item: %s", _device)
                continue

            # Always try to create button entities, even if they exist
            # The create_hgi_buttons function will handle duplicates
            hgi_buttons = _create_hgi_buttons(coordinator, _device)
            if hgi_buttons:
                _LOGGER.debug(
                    "Adding %d buttons for newly discovered HGIs",
                    len(hgi_buttons),
                )
                async_add_entities(hgi_buttons, update_before_add=False)
            else:
                _LOGGER.debug("No new HGI buttons registered")

            # The create_fan_buttons function will handle duplicates
            fan_buttons = _create_fan_buttons(coordinator, _device)
            if fan_buttons:
                _LOGGER.debug(
                    "Adding filter-reset button for newly discovered FAN %s",
                    _device.id,
                )
                async_add_entities(fan_buttons, update_before_add=False)
            else:
                _LOGGER.debug("No new FAN buttons registered")

    # Register the callback with the coordinator
    coordinator.async_register_platform(platform, add_devices)

    # Load any existing devices that were discovered before platform
    # registration
    coord_devices = getattr(coordinator, "devices", [])
    if coord_devices:
        # 1. Gateway-level service buttons for HGIs already known at setup time
        hgi_devices = [d for d in coord_devices if device_slug(d) == "HGI"]
        for _device in hgi_devices:
            buttons.extend(_create_hgi_buttons(coordinator, _device))

        # 2. Filter-reset buttons for FANs already known at setup time
        #
        fan_devices = [d for d in coord_devices if device_slug(d) == "FAN"]
        for _device in fan_devices:
            buttons.extend(_create_fan_buttons(coordinator, _device))

        if buttons:
            _LOGGER.debug("Adding %d button entities", len(buttons))
            async_add_entities(buttons, update_before_add=False)
        else:
            _LOGGER.debug("No button entities registered")

        # 3. Callback creates buttons for FAN devices discovered after setup (e.g. before the climate
        # platform registered the FAN entities, after the HGI came online or after a cache clear / re-discovery)
