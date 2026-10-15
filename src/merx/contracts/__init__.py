"""Load dataset contracts from YAML and check them against the reference data."""

from pathlib import Path

import yaml

from merx.config import CONTRACTS_DIR
from merx.contracts.model import Contract, check_name
from merx.reference import Reference

__all__ = ["Contract", "ContractError", "check_name", "load_contract", "load_contracts"]


class ContractError(ValueError):
    """A contract is invalid on its own or disagrees with the reference data."""


def load_contract(path: Path) -> Contract:
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    try:
        contract = Contract.model_validate(raw)
    except ValueError as e:
        raise ContractError(f"{path.name}: {e}") from e
    if contract.id != path.stem:
        raise ContractError(f"{path.name}: id {contract.id!r} must match the file name")
    return contract


def validate_against_reference(contract: Contract, reference: Reference) -> None:
    """The contract's instrument, unit and source identifier must all agree with reference."""
    instrument = reference.instruments.get(contract.instrument)
    if instrument is None:
        raise ContractError(f"{contract.id}: unknown instrument {contract.instrument}")
    if contract.unit != instrument.unit:
        raise ContractError(
            f"{contract.id}: unit {contract.unit} disagrees with instrument unit {instrument.unit}"
        )
    key = (contract.source.provider, contract.source.series_key)
    entry = reference.crosswalk.get(key)
    if entry is None:
        raise ContractError(f"{contract.id}: no crosswalk entry for {key[0]}:{key[1]}")
    if entry.instrument_id != contract.instrument:
        raise ContractError(
            f"{contract.id}: crosswalk maps {key[0]}:{key[1]} to {entry.instrument_id}, "
            f"not {contract.instrument}"
        )


def load_contracts(reference: Reference, directory: Path = CONTRACTS_DIR) -> list[Contract]:
    """Every contract in the directory, each validated against the reference data."""
    contracts = [load_contract(p) for p in sorted(directory.glob("*.yaml"))]
    if not contracts:
        raise ContractError(f"no contracts found in {directory}")
    for contract in contracts:
        validate_against_reference(contract, reference)
    return contracts
