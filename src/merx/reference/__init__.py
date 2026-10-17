"""Reference data: our own instrument identifiers, units, and the crosswalk that maps each
provider's identifiers onto them.

Joining on a provider's identifier, never on its display name, is the point of the
crosswalk: CFTC market 067651 has been published under more than one name.
"""

import csv
from dataclasses import dataclass
from pathlib import Path

from merx.config import REFERENCE_DIR


@dataclass(frozen=True)
class Instrument:
    instrument_id: str
    name: str
    commodity: str
    # The sector the client's menu groups by, and what sort of data this is
    # (spot, fundamental, positioning, environmental, curve, index).
    sector: str
    kind: str
    data_type: str
    location: str
    unit: str


@dataclass(frozen=True)
class Unit:
    unit_id: str
    description: str
    quantity: str


@dataclass(frozen=True)
class Provider:
    """Who publishes a source, how it is reached, and on what terms.

    The terms are the short form; ``docs/PROVENANCE.md`` holds the full record of what
    was read and when. Keeping them here as data means the client site can show, next to
    any series, where the numbers came from and what the licence asks of a user — which
    is the part of provenance that people actually need at the point of use.
    """

    provider_id: str
    name: str
    access: str
    terms: str
    terms_url: str
    attribution: str


@dataclass(frozen=True)
class CrosswalkEntry:
    provider: str
    source_key: str
    instrument_id: str
    source_label: str
    note: str


@dataclass(frozen=True)
class Reference:
    instruments: dict[str, Instrument]
    units: dict[str, Unit]
    crosswalk: dict[tuple[str, str], CrosswalkEntry]
    providers: dict[str, Provider]

    def resolve(self, provider: str, source_key: str) -> Instrument:
        """The instrument a provider's identifier maps to. Raises if it is not mapped."""
        try:
            entry = self.crosswalk[(provider, source_key)]
        except KeyError:
            raise KeyError(f"no crosswalk entry for {provider}:{source_key}") from None
        return self.instruments[entry.instrument_id]


class ReferenceError(ValueError):
    """The reference files are inconsistent."""


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _unique_index[T](rows: list[T], key, what: str) -> dict:
    index: dict = {}
    for row in rows:
        k = key(row)
        if k in index:
            raise ReferenceError(f"duplicate {what}: {k}")
        index[k] = row
    return index


def load_reference(directory: Path = REFERENCE_DIR) -> Reference:
    """Load and cross-check the reference files. Fails loudly on any inconsistency."""
    units = _unique_index(
        [Unit(**r) for r in _rows(directory / "units.csv")], lambda u: u.unit_id, "unit"
    )
    instruments = _unique_index(
        [Instrument(**r) for r in _rows(directory / "instruments.csv")],
        lambda i: i.instrument_id,
        "instrument",
    )
    crosswalk = _unique_index(
        [CrosswalkEntry(**r) for r in _rows(directory / "crosswalk.csv")],
        lambda c: (c.provider, c.source_key),
        "crosswalk key",
    )
    providers = _unique_index(
        [Provider(**r) for r in _rows(directory / "providers.csv")],
        lambda p: p.provider_id,
        "provider",
    )
    for inst in instruments.values():
        if inst.unit not in units:
            raise ReferenceError(f"instrument {inst.instrument_id} has unknown unit {inst.unit}")
    for entry in crosswalk.values():
        if entry.instrument_id not in instruments:
            raise ReferenceError(
                f"crosswalk {entry.provider}:{entry.source_key} points at unknown "
                f"instrument {entry.instrument_id}"
            )
        # A source whose terms are not recorded cannot be described to a user, which is
        # the same reason nothing is published from an unread licence.
        if entry.provider not in providers:
            raise ReferenceError(
                f"crosswalk {entry.provider}:{entry.source_key} names provider "
                f"{entry.provider!r}, which providers.csv does not describe"
            )
    return Reference(instruments=instruments, units=units, crosswalk=crosswalk, providers=providers)
