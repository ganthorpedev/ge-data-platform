"""Registry and step-execution error types."""

from __future__ import annotations


class RegistryError(ValueError):
    """Invalid step registration or lookup."""


class UnknownStepError(RegistryError):
    """No step with this key is registered in this process."""


class StepVersionMismatchError(RegistryError):
    """The requested step version differs from the version registered in code."""


class PermanentStepError(Exception):
    """Raised by a step for a failure that retrying cannot fix (bad input, failed validation).

    The attempt is marked FAILED and the task is NOT retried.
    """


class RetryableStepError(Exception):
    """Raised by a step to signal an explicitly transient failure.

    Any other unexpected exception is also treated as retryable (bounded by
    the node's max_attempts); this class exists for clarity at call sites.
    """
