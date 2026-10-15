from pathlib import Path

import pytest

from merx.contracts import (
    ContractError,
    load_contract,
    load_contracts,
    validate_against_reference,
)
from merx.reference import ReferenceError, load_reference

VALID = """
id: {id}
title: Test series
domain: oil
owner: oil-data-steward
tier: 1
frequency: daily
instrument: WTI_CUSHING_SPOT
unit: USD/bbl
source:
  provider: eia
  format: eia_hist_xls
  url: https://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls
  series_key: RWTC
schedule:
  cron: "30 18 * * 1-5"
checks:
  - {{type: not_null, column: value}}
"""


def write(tmp_path: Path, name: str, text: str) -> Path:
    path = tmp_path / f"{name}.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_repository_contracts_and_reference_are_consistent():
    # The real gate: every committed contract loads and agrees with the reference data.
    contracts = load_contracts(load_reference())
    assert {c.id for c in contracts} >= {"eia_wti_spot", "eia_brent_spot"}


def test_minimal_contract_loads(tmp_path):
    contract = load_contract(write(tmp_path, "test_series", VALID.format(id="test_series")))
    assert contract.schedule.timezone == "UTC"
    validate_against_reference(contract, load_reference())


def test_unknown_key_is_rejected(tmp_path):
    text = VALID.format(id="test_series") + "colour: blue\n"
    with pytest.raises(ContractError, match="colour"):
        load_contract(write(tmp_path, "test_series", text))


def test_id_must_match_file_name(tmp_path):
    with pytest.raises(ContractError, match="must match the file name"):
        load_contract(write(tmp_path, "other_name", VALID.format(id="test_series")))


def test_check_on_unknown_column_is_rejected(tmp_path):
    text = VALID.format(id="t").replace("column: value", "column: price")
    with pytest.raises(ContractError):
        load_contract(write(tmp_path, "t", text))


def test_inverted_range_is_rejected(tmp_path):
    text = VALID.format(id="t").replace(
        "{type: not_null, column: value}", "{type: range, column: value, min: 5, max: 1}"
    )
    with pytest.raises(ContractError, match="must be below max"):
        load_contract(write(tmp_path, "t", text))


def test_duplicate_checks_are_rejected(tmp_path):
    text = VALID.format(id="t") + "  - {type: not_null, column: value, severity: warn}\n"
    with pytest.raises(ContractError, match="duplicate checks"):
        load_contract(write(tmp_path, "t", text))


def test_unknown_instrument_is_rejected(tmp_path):
    text = VALID.format(id="t").replace("WTI_CUSHING_SPOT", "NO_SUCH_THING")
    with pytest.raises(ContractError, match="unknown instrument"):
        validate_against_reference(load_contract(write(tmp_path, "t", text)), load_reference())


def test_unit_must_match_instrument(tmp_path):
    text = VALID.format(id="t").replace("unit: USD/bbl", "unit: USD/MMBtu")
    with pytest.raises(ContractError, match="disagrees with instrument unit"):
        validate_against_reference(load_contract(write(tmp_path, "t", text)), load_reference())


def test_series_key_must_map_to_the_same_instrument(tmp_path):
    # RBRTE is in the crosswalk, but it maps to Brent, not WTI.
    text = VALID.format(id="t").replace("series_key: RWTC", "series_key: RBRTE")
    with pytest.raises(ContractError, match="maps eia:RBRTE to BRENT_SPOT"):
        validate_against_reference(load_contract(write(tmp_path, "t", text)), load_reference())


def test_crosswalk_resolves_cftc_market_by_code_not_name():
    reference = load_reference()
    assert reference.resolve("cftc", "067651").instrument_id == "WTI_NYMEX_FUTURES"


def test_reference_rejects_crosswalk_to_unknown_instrument(tmp_path):
    for name in ("units.csv", "instruments.csv"):
        (tmp_path / name).write_text(
            (Path(__file__).parents[1] / "reference" / name).read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    (tmp_path / "crosswalk.csv").write_text(
        "provider,source_key,instrument_id,source_label,note\neia,X,NOPE,x,\n", encoding="utf-8"
    )
    with pytest.raises(ReferenceError, match="unknown instrument NOPE"):
        load_reference(tmp_path)
