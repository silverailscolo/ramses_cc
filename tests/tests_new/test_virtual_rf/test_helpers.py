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
    dev._gwy = MagicMock()  # Mock the gateway so BindingManager can find async_send_cmd

    # Create a dummy Fakeable class to patch in
    class MockFakeable:
        pass

    # Patch Fakeable to verify it gets mixed in
    with (
        patch("tests.virtual_rf.helpers.Fakeable", MockFakeable),
        patch(
            "tests.virtual_rf.helpers.BindingManager"
        ),  # Prevent actual instantiation
    ):
        helpers.ensure_fakeable(dev, make_fake=False)

        # Check that the device class now inherits from the mixin
        assert issubclass(dev.__class__, MockFakeable)
        assert hasattr(dev, "_bind_context")


async def test_ensure_fakeable_calls_make_fake() -> None:
    """Test that ensure_fakeable calls _make_fake when requested."""
    # This must be async because BindContext(dev) calls asyncio.get_running_loop()

    class DummyDevice(Device):
        def __init__(self) -> None:
            pass

        def _make_fake(self) -> None:
            pass

    dev = DummyDevice()
    dev._gwy = MagicMock()  # Mock the gateway so BindingManager can find async_send_cmd

    class MockFakeable:
        pass

    # Use patch.object to mock the method safely without triggering mypy method-assign error
    with (
        patch.object(dev, "_make_fake") as mock_make_fake,
        patch("tests.virtual_rf.helpers.Fakeable", MockFakeable),
        patch(
            "tests.virtual_rf.helpers.BindingManager"
        ),  # Prevent actual instantiation
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
    # Does not need _gwy mocked because it returns early before BindingManager instantiation

    with patch("tests.virtual_rf.helpers.Fakeable", MockFakeable):
        # Should simply return without error or modification
        helpers.ensure_fakeable(dev, make_fake=False)

        # Verify it exited early (no _bind_context should be assigned)
        assert not hasattr(dev, "_bind_context")
