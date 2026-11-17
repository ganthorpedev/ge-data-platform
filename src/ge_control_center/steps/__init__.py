"""Registered steps. Importing this package registers every step below.

This list is the ONLY way code enters the step registry. Adding a step means
adding a module here, in code review -- never through the API or the
visual editor.
"""

from ge_control_center.steps import builtin, warehouse  # noqa: F401

__all__ = ["builtin", "warehouse"]
