"""Typed errors raised by the Saude (dadosabertos.saude.gov.br) client."""


class SaudeError(Exception):
    """Base class for all errors raised by the Saude client."""


class BuildIdMissing(SaudeError):
    """The ``__NEXT_DATA__`` block could not be located on the homepage."""


class PortalChanged(SaudeError):
    """The catalog response no longer matches the expected Next.js schema."""


class DatasetNotFound(SaudeError):
    """The requested dataset slug does not exist on the portal."""


class ResourceNotFound(SaudeError):
    """No downloadable resource matched the given selector."""

    def __init__(self, message: str, *, candidates: int = 0) -> None:
        super().__init__(message)
        self.candidates = candidates


class NoUsableBuildId(SaudeError):
    """The buildId cache and the live homepage both failed to yield a value."""
