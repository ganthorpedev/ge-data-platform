"""Architecture guard: the GE Data Control Center has zero Dagster dependencies.

Fails if any ge_control_center module imports dagster or the legacy
ge_data_platform.orchestration package (directly or transitively), or if the
Control Center's runtime requirements mention dagster.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_DIR = PROJECT_ROOT / "src" / "ge_control_center"

_IMPORT_CHECK = r"""
import importlib, pkgutil, sys
import ge_control_center
for module in pkgutil.walk_packages(ge_control_center.__path__, "ge_control_center."):
    importlib.import_module(module.name)
bad = sorted(m for m in sys.modules if m == "dagster" or m.startswith(("dagster.", "dagster_"))
             or m == "ge_data_platform.orchestration" or m.startswith("ge_data_platform.orchestration."))
print("\n".join(bad))
sys.exit(1 if bad else 0)
"""


def test_importing_every_control_center_module_never_loads_dagster() -> None:
    result = subprocess.run([sys.executable, "-c", _IMPORT_CHECK], capture_output=True, text=True, cwd=PROJECT_ROOT)
    assert result.returncode == 0, f"forbidden modules imported:\n{result.stdout}\n{result.stderr}"


def test_no_dagster_import_statements_in_control_center_source() -> None:
    pattern = re.compile(r"^\s*(import|from)\s+(dagster\b|dagster_|ge_data_platform\.orchestration\b)", re.MULTILINE)
    offenders = [str(path.relative_to(PROJECT_ROOT)) for path in PACKAGE_DIR.rglob("*.py")
                 if pattern.search(path.read_text(encoding="utf-8"))]
    assert offenders == []


def test_control_center_requirements_exclude_dagster() -> None:
    requirements = (PROJECT_ROOT / "requirements" / "control-center.txt").read_text(encoding="utf-8").lower()
    assert "dagster" not in "\n".join(line for line in requirements.splitlines() if not line.strip().startswith("#"))


def test_control_center_image_build_rejects_dagster() -> None:
    dockerfile = (PROJECT_ROOT / "deploy" / "docker" / "Dockerfile").read_text(encoding="utf-8")
    assert "--no-deps" in dockerfile
    assert "find_spec('dagster')" in dockerfile
