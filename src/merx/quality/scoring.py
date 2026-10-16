"""Score a detector's flags against the ground-truth manifest.

Three numbers per run, each answering a question an operations lead would ask:

* **recall**: of the seeded defects, how many got at least one flag? (A defect counts
  as caught if any of its dates is flagged: one alert is enough to send someone to it.)
* **precision**: of the flags that were not already raised on the untouched data, how
  many point at a seeded defect? Flags on dates the detector also flags in the real,
  untouched series are set aside: they are real-world alerts (a market event, or a
  genuine error in the source) that only a person can judge, not evidence about the
  seeded defects.
* **alerts per year on real data**: how often the detector fires on the untouched
  series. The cost of a detector in reviewer time.
"""

from dataclasses import dataclass
from datetime import date

from merx.quality.defects import Defect


@dataclass(frozen=True)
class Score:
    defects: int
    caught: int
    flags: int
    true_flags: int
    induced_false_flags: int
    real_data_flags_seen: int  # flags on corrupted data that also fire on clean data

    @property
    def recall(self) -> float | None:
        return self.caught / self.defects if self.defects else None

    @property
    def precision(self) -> float | None:
        judged = self.true_flags + self.induced_false_flags
        return self.true_flags / judged if judged else None


def score(flags: set[date], clean_flags: set[date], defects: list[Defect]) -> Score:
    defect_dates = {d for defect in defects for d in defect.dates}
    true_flags = flags & defect_dates
    other = flags - defect_dates
    return Score(
        defects=len(defects),
        caught=sum(1 for defect in defects if flags.intersection(defect.dates)),
        flags=len(flags),
        true_flags=len(true_flags),
        induced_false_flags=len(other - clean_flags),
        real_data_flags_seen=len(other & clean_flags),
    )


def caught_by_type(flags: set[date], defects: list[Defect]) -> dict[str, tuple[int, int]]:
    """(caught, total) per defect type."""
    out: dict[str, tuple[int, int]] = {}
    for defect in defects:
        caught, total = out.get(defect.defect_type, (0, 0))
        out[defect.defect_type] = (caught + bool(flags.intersection(defect.dates)), total + 1)
    return out
