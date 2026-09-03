"""Quality scale and architectural standards tests for Home Assistant IQS compliance.

Tests Bronze, Silver, and Platinum standards to guarantee continuous compliance
and prevent regressions across pull requests.
"""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path

PLATFORMS = (
    "binary_sensor.py",
    "climate.py",
    "event.py",
    "number.py",
    "remote.py",
    "sensor.py",
    "water_heater.py",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = REPO_ROOT / "custom_components" / "ramses_cc"
TESTS_DIR = REPO_ROOT / "tests"


# ==============================================================================
# 🥉 BRONZE QUALITY SCALE TESTS
# ==============================================================================


def test_bronze_has_entity_name_on_all_entities() -> None:
    """Verify that every entity class declares _attr_has_entity_name = True."""
    # Arrange
    violations: list[str] = []

    known_entity_bases = {
        "RamsesEntity",
        "RamsesBinarySensor",
        "RamsesController",
        "RamsesEvent",
        "RamsesHvac",
        "RamsesNumberBase",
        "RamsesRemote",
        "RamsesSensor",
        "RamsesWaterHeater",
        "RamsesZone",
    }

    # Act
    for platform in PLATFORMS:
        platform_file = PACKAGE_DIR / platform
        assert platform_file.exists(), (
            f"Platform file {platform} does not exist"
        )

        tree = ast.parse(platform_file.read_text(encoding="utf-8"))

        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue

            base_names = [
                b.id
                if isinstance(b, ast.Name)
                else b.attr
                if isinstance(b, ast.Attribute)
                else ""
                for b in node.bases
            ]
            is_entity_class = any(
                name.endswith("Entity") or name.startswith("Ramses")
                for name in base_names
            )
            if not is_entity_class or node.name.endswith("EntityDescription"):
                continue

            inherits_has_entity_name = any(
                b in known_entity_bases for b in base_names
            )
            has_attr = False
            for item in node.body:
                if isinstance(item, ast.Assign):
                    for target in item.targets:
                        if getattr(target, "id", None) in (
                            "_attr_has_entity_name",
                            "has_entity_name",
                        ):
                            if (
                                isinstance(item.value, ast.Constant)
                                and item.value.value is True
                            ):
                                has_attr = True
                                break

            if inherits_has_entity_name or has_attr:
                known_entity_bases.add(node.name)
            else:
                violations.append(f"{platform}: Class '{node.name}'")

    # Assert
    assert not violations, (
        "Bronze IQS violation: Entity classes missing has_entity_name = True:\n"
        + "\n".join(violations)
    )


def test_bronze_runtime_data_pattern() -> None:
    """Verify runtime data is stored in entry.runtime_data without legacy hass.data."""
    # Arrange
    init_file = PACKAGE_DIR / "__init__.py"
    init_content = init_file.read_text(encoding="utf-8")
    legacy_violations: list[str] = []

    # Act - 1. Check entry.runtime_data assignment in __init__.py
    has_runtime_data_assign = "entry.runtime_data = " in init_content

    # Act - 2. Check for legacy hass.data[DOMAIN][entry.entry_id] in package
    for root, _, files in os.walk(PACKAGE_DIR):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            file_path = Path(root) / filename
            content = file_path.read_text(encoding="utf-8")
            for line_number, line in enumerate(content.splitlines(), 1):
                if "hass.data[DOMAIN][" in line and "entry" in line:
                    rel_path = file_path.relative_to(REPO_ROOT)
                    legacy_violations.append(
                        f"{rel_path}:{line_number}: {line.strip()}"
                    )

    # Assert
    assert has_runtime_data_assign, (
        "Bronze IQS violation: entry.runtime_data assignment missing from __init__.py"
    )
    assert not legacy_violations, (
        "Bronze IQS violation: Legacy hass.data[DOMAIN][entry_id] usage found:\n"
        + "\n".join(legacy_violations)
    )


def test_bronze_action_setup_in_async_setup() -> None:
    """Verify platform entity services are registered in async_setup."""
    # Arrange
    init_file = PACKAGE_DIR / "__init__.py"
    content = init_file.read_text(encoding="utf-8")
    tree = ast.parse(content, filename=str(init_file))

    services_in_async_setup = False
    services_in_async_setup_entry = False

    # Act
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name == "async_setup":
                body_code = ast.get_source_segment(content, node) or ""
                if "async_register_platform_entity_service" in body_code:
                    services_in_async_setup = True
            elif node.name == "async_setup_entry":
                body_code = ast.get_source_segment(content, node) or ""
                if "async_register_platform_entity_service" in body_code:
                    services_in_async_setup_entry = True

    # Assert
    assert services_in_async_setup, (
        "Bronze IQS violation: Services must be registered in async_setup"
    )
    assert not services_in_async_setup_entry, (
        "Bronze IQS violation: Services must not be registered in async_setup_entry"
    )


def test_bronze_manifest_metadata_and_transparency() -> None:
    """Verify manifest.json declares all required Bronze metadata fields."""
    # Arrange
    manifest_file = PACKAGE_DIR / "manifest.json"
    assert manifest_file.exists(), f"Missing manifest file: {manifest_file}"

    # Act
    with open(manifest_file, encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)

    # Assert
    assert manifest.get("domain") == "ramses_cc"
    assert manifest.get("name")
    assert manifest.get("config_flow") is True
    assert manifest.get("documentation", "").startswith("https://")
    assert manifest.get("issue_tracker", "").startswith("https://")
    codeowners = manifest.get("codeowners", [])
    assert isinstance(codeowners, list) and codeowners
    assert all(owner.startswith("@") for owner in codeowners)
    requirements = manifest.get("requirements", [])
    assert isinstance(requirements, list) and requirements


# ==============================================================================
# 🥈 SILVER QUALITY SCALE TESTS
# ==============================================================================


def test_silver_parallel_updates_on_all_entity_platforms() -> None:
    """Verify that PARALLEL_UPDATES = 0 is declared across all entity platforms."""
    # Arrange
    missing_platforms: list[str] = []

    # Act
    for platform in PLATFORMS:
        platform_file = PACKAGE_DIR / platform
        assert platform_file.exists(), (
            f"Platform file {platform} does not exist"
        )

        tree = ast.parse(platform_file.read_text(encoding="utf-8"))
        has_parallel_updates = False

        for node in tree.body:
            if isinstance(node, ast.AnnAssign):
                if getattr(node.target, "id", None) == "PARALLEL_UPDATES":
                    if (
                        isinstance(node.value, ast.Constant)
                        and node.value.value == 0
                    ):
                        has_parallel_updates = True
                        break
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if getattr(target, "id", None) == "PARALLEL_UPDATES":
                        if (
                            isinstance(node.value, ast.Constant)
                            and node.value.value == 0
                        ):
                            has_parallel_updates = True
                            break

        if not has_parallel_updates:
            missing_platforms.append(platform)

    # Assert
    assert not missing_platforms, (
        f"Silver IQS violation: PARALLEL_UPDATES = 0 missing from {missing_platforms}"
    )


def test_silver_test_lifecycle_zero_private_mutations() -> None:
    """Verify zero private _entries mutations across all test files."""
    # Arrange
    violations: list[str] = []

    # Act
    for root, _, files in os.walk(TESTS_DIR):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            file_path = Path(root) / filename
            if file_path.resolve() == Path(__file__).resolve():
                continue
            content = file_path.read_text(encoding="utf-8")
            for line_number, line in enumerate(content.splitlines(), 1):
                if "_entries.pop" in line or "._entries[" in line:
                    rel_path = file_path.relative_to(REPO_ROOT)
                    violations.append(
                        f"{rel_path}:{line_number}: {line.strip()}"
                    )

    # Assert
    assert not violations, (
        "Silver IQS violation: Private _entries mutations found in tests:\n"
        + "\n".join(violations)
    )


def test_silver_conftest_auto_cleanup_fixture() -> None:
    """Verify auto_cleanup_config_entries is an async fixture using public APIs."""
    # Arrange
    conftest_file = TESTS_DIR / "tests_new" / "conftest.py"
    assert conftest_file.exists(), f"Missing conftest: {conftest_file}"

    content = conftest_file.read_text(encoding="utf-8")
    tree = ast.parse(content, filename=str(conftest_file))

    fixture_found = False
    is_async = False
    has_async_unload = False
    has_async_remove = False
    has_block_till_done = False

    # Act
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.AsyncFunctionDef)
            and node.name == "auto_cleanup_config_entries"
        ):
            fixture_found = True
            is_async = True
            body_code = ast.get_source_segment(content, node) or ""
            if "async_unload" in body_code:
                has_async_unload = True
            if "async_remove" in body_code:
                has_async_remove = True
            if "async_block_till_done" in body_code:
                has_block_till_done = True
        elif (
            isinstance(node, ast.FunctionDef)
            and node.name == "auto_cleanup_config_entries"
        ):
            fixture_found = True
            is_async = False

    # Assert
    assert fixture_found, "auto_cleanup_config_entries fixture not found"
    assert is_async, "auto_cleanup_config_entries must be an async def fixture"
    assert has_async_unload, (
        "auto_cleanup_config_entries must await async_unload"
    )
    assert has_async_remove, (
        "auto_cleanup_config_entries must await async_remove"
    )
    assert has_block_till_done, (
        "auto_cleanup_config_entries must await async_block_till_done"
    )


def test_silver_production_setup_api_purity() -> None:
    """Verify production setup methods do not expose test-specific flag arguments."""
    # Arrange
    coordinator_file = PACKAGE_DIR / "coordinator.py"
    init_file = PACKAGE_DIR / "__init__.py"
    violations: list[str] = []

    # Act
    for file_path in (coordinator_file, init_file):
        assert file_path.exists(), f"Missing file: {file_path}"
        content = file_path.read_text(encoding="utf-8")
        tree = ast.parse(content, filename=str(file_path))

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name in (
                    "async_setup",
                    "async_start",
                    "async_setup_entry",
                    "_async_start_discovery_scan",
                ):
                    argument_names = [arg.arg for arg in node.args.args]
                    if (
                        "discovery" in argument_names
                        or "is_test" in argument_names
                    ):
                        rel_path = file_path.relative_to(REPO_ROOT)
                        violations.append(
                            f"{rel_path}:{node.lineno}: Method '{node.name}' has test-flag argument '{argument_names}'"
                        )

    # Assert
    assert not violations, (
        "Silver IQS violation: Production setup APIs have test flags:\n"
        + "\n".join(violations)
    )


# ==============================================================================
# 🏅 PLATINUM QUALITY SCALE TESTS
# ==============================================================================


def test_platinum_async_dependency_and_no_blocking_io() -> None:
    """Verify integration has no synchronous blocking I/O calls or dependencies."""
    # Arrange
    banned_imports = ("requests", "urllib.request", "http.client")
    violations: list[str] = []

    # Act
    for root, _, files in os.walk(PACKAGE_DIR):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            file_path = Path(root) / filename
            content = file_path.read_text(encoding="utf-8")
            tree = ast.parse(content, filename=str(file_path))

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if any(
                            alias.name.startswith(b) for b in banned_imports
                        ):
                            rel_path = file_path.relative_to(REPO_ROOT)
                            violations.append(
                                f"{rel_path}:{node.lineno}: Synchronous import '{alias.name}'"
                            )
                elif isinstance(node, ast.ImportFrom):
                    mod_name = node.module or ""
                    if any(mod_name.startswith(b) for b in banned_imports):
                        rel_path = file_path.relative_to(REPO_ROOT)
                        violations.append(
                            f"{rel_path}:{node.lineno}: Synchronous import from '{mod_name}'"
                        )
                elif isinstance(node, ast.Call):
                    func = node.func
                    if (
                        isinstance(func, ast.Attribute)
                        and func.attr == "sleep"
                        and isinstance(func.value, ast.Name)
                        and func.value.id == "time"
                    ):
                        rel_path = file_path.relative_to(REPO_ROOT)
                        violations.append(
                            f"{rel_path}:{node.lineno}: Banned blocking time.sleep() call"
                        )

    # Assert
    assert not violations, (
        "Platinum IQS violation: Blocking synchronous I/O found in custom_components:\n"
        + "\n".join(violations)
    )


def test_platinum_inject_websession_invariant() -> None:
    """Verify custom_components never instantiates un-injected aiohttp.ClientSession."""
    # Arrange
    violations: list[str] = []

    # Act
    for root, _, files in os.walk(PACKAGE_DIR):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            file_path = Path(root) / filename
            content = file_path.read_text(encoding="utf-8")
            tree = ast.parse(content, filename=str(file_path))

            for node in ast.walk(tree):
                if isinstance(node, ast.Call):
                    func = node.func
                    if (
                        isinstance(func, ast.Attribute)
                        and func.attr == "ClientSession"
                    ) or (
                        isinstance(func, ast.Name)
                        and func.id == "ClientSession"
                    ):
                        rel_path = file_path.relative_to(REPO_ROOT)
                        violations.append(
                            f"{rel_path}:{node.lineno}: Un-injected ClientSession() call"
                        )

    # Assert
    assert not violations, (
        "Platinum IQS violation: Direct aiohttp.ClientSession() instantiations found. "
        "Must use async_get_clientsession(hass) from homeassistant.helpers.aiohttp_client:\n"
        + "\n".join(violations)
    )


def test_platinum_strict_typing_configuration_and_syntax() -> None:
    """Verify strict mypy configuration and absence of legacy typing imports."""
    # Arrange
    pyproject_file = REPO_ROOT / "pyproject.toml"
    assert pyproject_file.exists(), f"Missing pyproject.toml: {pyproject_file}"

    content = pyproject_file.read_text(encoding="utf-8")
    banned_typing_aliases = (
        "Optional",
        "Union",
        "List",
        "Dict",
        "Set",
        "Tuple",
    )
    violations: list[str] = []

    # Act - 1. Verify strict mypy flags in pyproject.toml
    assert "disallow_untyped_defs = true" in content, (
        "Platinum IQS violation: mypy disallow_untyped_defs must be true"
    )
    assert "disallow_incomplete_defs = true" in content, (
        "Platinum IQS violation: mypy disallow_incomplete_defs must be true"
    )
    assert "check_untyped_defs = true" in content, (
        "Platinum IQS violation: mypy check_untyped_defs must be true"
    )
    assert "strict_equality = true" in content, (
        "Platinum IQS violation: mypy strict_equality must be true"
    )

    # Act - 2. Scan custom_components for banned legacy typing aliases
    for root, _, files in os.walk(PACKAGE_DIR):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            file_path = Path(root) / filename
            file_content = file_path.read_text(encoding="utf-8")
            tree = ast.parse(file_content, filename=str(file_path))

            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.module == "typing"
                ):
                    for alias in node.names:
                        if alias.name in banned_typing_aliases:
                            rel_path = file_path.relative_to(REPO_ROOT)
                            violations.append(
                                f"{rel_path}:{node.lineno}: Banned typing import '{alias.name}'"
                            )

    # Assert
    assert not violations, (
        "Platinum IQS violation: Legacy typing aliases found in custom_components "
        "(use Python 3.13+ syntax: |, list, dict):\n" + "\n".join(violations)
    )
