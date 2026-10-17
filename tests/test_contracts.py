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
measures: [price]
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


def reference_copy(tmp_path: Path, **replace: str) -> Path:
    """The repository's reference files, with any one of them replaced."""
    for name in ("units.csv", "instruments.csv", "crosswalk.csv", "providers.csv"):
        (tmp_path / name).write_text(
            replace.get(name.removesuffix(".csv"))
            or (Path(__file__).parents[1] / "reference" / name).read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    return tmp_path


CROSSWALK = """provider,source_key,instrument_id,source_label,note
{row}
"""


def test_reference_rejects_crosswalk_to_unknown_instrument(tmp_path):
    directory = reference_copy(tmp_path, crosswalk=CROSSWALK.format(row="eia,X,NOPE,x,"))
    with pytest.raises(ReferenceError, match="unknown instrument NOPE"):
        load_reference(directory)


def test_reference_rejects_a_source_whose_publisher_is_not_described(tmp_path):
    """Nothing is published from a source whose terms have not been recorded, so a
    provider with no entry in providers.csv is an error, not a blank space on the page."""
    directory = reference_copy(
        tmp_path, crosswalk=CROSSWALK.format(row="nobody,X,WTI_CUSHING_SPOT,x,")
    )
    with pytest.raises(ReferenceError, match="providers.csv does not describe"):
        load_reference(directory)


def test_every_provider_a_contract_uses_is_described():
    reference = load_reference()
    for contract in load_contracts(reference):
        provider = reference.providers[contract.source.provider]
        assert provider.name and provider.terms and provider.terms_url
        assert provider.attribution


def test_single_series_format_needs_exactly_one_measure(tmp_path):
    text = VALID.format(id="t").replace("measures: [price]", "measures: [price, volume]")
    with pytest.raises(ContractError, match="holds one series"):
        load_contract(write(tmp_path, "t", text))


def test_range_check_on_unknown_measure_is_rejected(tmp_path):
    text = VALID.format(id="t").replace(
        "{type: not_null, column: value}",
        "{type: range, column: value, min: 0, max: 1, measure: volume}",
    )
    with pytest.raises(ContractError, match="unknown measure 'volume'"):
        load_contract(write(tmp_path, "t", text))


def test_unique_check_must_include_the_date(tmp_path):
    text = VALID.format(id="t").replace(
        "{type: not_null, column: value}", "{type: unique, columns: [measure]}"
    )
    with pytest.raises(ContractError, match="must include valid_date"):
        load_contract(write(tmp_path, "t", text))


CFTC = (Path(__file__).parents[1] / "contracts" / "cftc_wti_positions.yaml").read_text(
    encoding="utf-8"
)


def cftc_variant(tmp_path, old: str, new: str):
    text = CFTC.replace("id: cftc_wti_positions", "id: t")
    assert old in text
    return load_contract(write(tmp_path, "t", text.replace(old, new, 1)))


def test_year_placeholder_needs_partitions(tmp_path):
    with pytest.raises(ContractError, match="go together"):
        cftc_variant(tmp_path, "  partitions: {by: year, start: 2010}\n", "")


def test_columns_must_map_every_measure(tmp_path):
    with pytest.raises(ContractError, match="map every measure"):
        cftc_variant(tmp_path, "    swap_spread: Swap__Positions_Spread_All\n", "")


def test_single_series_format_takes_no_columns(tmp_path):
    text = VALID.format(id="t").replace(
        "  series_key: RWTC\n", "  series_key: RWTC\n  columns: {price: Value}\n"
    )
    with pytest.raises(ContractError, match="takes no columns"):
        load_contract(write(tmp_path, "t", text))


def test_balance_check_on_unknown_measure_is_rejected(tmp_path):
    with pytest.raises(ContractError, match="unknown measures"):
        cftc_variant(tmp_path, "    total: open_interest\n", "    total: open_interests\n")
