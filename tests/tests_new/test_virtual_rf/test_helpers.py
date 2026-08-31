"""Tests for the virtual_rf.helpers module."""

from unittest.mock import MagicMock, patch

from ramses_rf import Device
from tests.virtual_rf import helpers


async def test_ensure_fakeable_modifies_class() -> None:
    """Test that ensure_fakeable mixes in the Fakeable class."""

    # Use a real dummy class instead of MagicMock to avoid metaclass conflicts
    # when dynamic class creation occurs.
    class DummyDevice(Device):
        def __init__(self) -> None:
            pass  # Skip normal init

    dev = DummyDevice()
    dev._gateway = MagicMock()  # Mock the gateway on the device instance

    # Create a dummy Fakeable class to patch in
    class MockFakeable:
        pass

    # Patch Fakeable to verify it gets mixed in
    with patch("tests.virtual_rf.helpers.Fakeable", MockFakeable):
        helpers.ensure_fakeable(dev, make_fake=False)

        # Check that the device class now inherits from the mixin
        assert issubclass(dev.__class__, MockFakeable)


async def test_ensure_fakeable_calls_make_fake() -> None:
    """Test that ensure_fakeable calls _make_fake when requested."""
    # This is async because device faking requires an active asyncio event loop

    class DummyDevice(Device):
        def __init__(self) -> None:
            pass

        def _make_fake(self) -> None:
            pass

    dev = DummyDevice()
    dev._gateway = MagicMock()  # Mock the gateway on the device instance

    class MockFakeable:
        pass

    # Use patch.object to mock the method safely without triggering mypy method-assign error
    with (
        patch.object(dev, "_make_fake") as mock_make_fake,
        patch("tests.virtual_rf.helpers.Fakeable", MockFakeable),
    ):
        helpers.ensure_fakeable(dev, make_fake=True)

        mock_make_fake.assert_called_once()


async def test_ensure_fakeable_idempotent() -> None:
    """Test that ensure_fakeable does nothing if already fakeable."""

    class MockFakeable:
        pass

    class FakeableDevice(Device, MockFakeable):
        def __init__(self) -> None:
            pass

    dev = FakeableDevice()
    # Does not need _gateway mocked because it returns early for already-fakeable devices

    with patch("tests.virtual_rf.helpers.Fakeable", MockFakeable):
        # Should simply return without error or modification
        helpers.ensure_fakeable(dev, make_fake=False)
