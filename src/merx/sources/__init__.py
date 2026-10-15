"""Source adapters: fetching bytes from providers, and parsers for each file format."""


class SourceFormatError(ValueError):
    """A file does not have the layout its parser expects.

    Raised instead of guessing, so a provider's format change stops the pipeline at the
    parse step rather than flowing through as wrong data.
    """
