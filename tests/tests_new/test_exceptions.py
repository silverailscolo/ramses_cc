"""Tests for RAMSES CC custom integration exception hierarchy."""

from __future__ import annotations

import pytest
from homeassistant.exceptions import HomeAssistantError

from custom_components.ramses_cc.exceptions import (
    RamsesBindingError,
    RamsesDeviceError,
    RamsesError,
    RamsesProtocolError,
    RamsesScheduleError,
    RamsesSchemaError,
    RamsesTransportError,
)


def test_ramses_error_hierarchy() -> None:
    """Test that all integration exception classes subclass HomeAssistantError."""
    exceptions = [
        RamsesError,
        RamsesTransportError,
        RamsesProtocolError,
        RamsesDeviceError,
        RamsesBindingError,
        RamsesScheduleError,
        RamsesSchemaError,
    ]

    for exc_cls in exceptions:
        assert issubclass(exc_cls, HomeAssistantError)
        assert issubclass(exc_cls, RamsesError)


def test_ramses_error_instantiation() -> None:
    """Test instantiation and string representation of custom exceptions."""
    err = RamsesBindingError("Binding failed")
    assert str(err) == "Binding failed"
    assert isinstance(err, HomeAssistantError)
    assert isinstance(err, RamsesError)


def test_exception_catching_by_base_type() -> None:
    """Test catching specific Ramses exceptions via RamsesError base class."""
    with pytest.raises(RamsesError):
        raise RamsesProtocolError("Timeout waiting for packet")

    with pytest.raises(HomeAssistantError):
        raise RamsesTransportError("Serial port disconnected")
