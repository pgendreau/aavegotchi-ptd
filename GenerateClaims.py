#!/usr/bin/env python3
import sys
import csv
import json
from decimal import Decimal, InvalidOperation, getcontext
from typing import List, Dict, Any, Tuple

from eth_abi import encode
from eth_utils import keccak, to_checksum_address

getcontext().prec = 80  # high precision for Decimal math

WEI_PER_ETH = Decimal("1000000000000000000")


def parse_amount_to_wei(value: str, unit: str) -> int:
    """
    Convert rewardTotal to integer wei.
    - unit="eth": accepts decimals up to 18 places, converts exactly to wei
    - unit="wei": must be an integer string
    """
    v = (value or "").strip()
    if unit == "wei":
        if v.startswith("+"):
            v = v[1:]
        if not v.isdigit():
            raise ValueError(
                f"rewardTotal must be an integer wei string when unit=wei (got: {value!r})"
            )
        return int(v)

    # unit == "eth"
    try:
        d = Decimal(v)
    except InvalidOperation as e:
        raise ValueError(f"rewardTotal is not a valid decimal ETH amount: {value!r}") from e

    if d < 0:
        raise ValueError(f"rewardTotal cannot be negative (got: {value!r})")

    # Enforce <= 18 decimal places to avoid silent rounding
    exp = -d.as_tuple().exponent if d.as_tuple().exponent < 0 else 0
    if exp > 18:
        raise ValueError(f"rewardTotal has more than 18 decimals (got {exp}): {value!r}")

    wei = d * WEI_PER_ETH
    if wei != wei.to_integral_value():
        raise ValueError(f"rewardTotal cannot be represented exactly in wei: {value!r}")
    return int(wei)


def hash_leaf(index: int, account: str, amount_wei: int) -> bytes:
    """
    Must match Solidity in your contract:

      bytes32 leaf = keccak256(bytes.concat(keccak256(abi.encode(account, amount))));

    Notes:
    - index is NOT part of the leaf for this contract (kept only to preserve script structure / proof indexing).
    - This is a "double keccak": keccak( keccak(abi.encode(account, amount)) ).
    """
    inner_encoded = encode(["address", "uint256"], [account, amount_wei])
    inner_hash = keccak(inner_encoded)  # keccak256(abi.encode(account, amount))
    return keccak(inner_hash)           # keccak256(bytes.concat(inner_hash))


def hash_pair(a: bytes, b: bytes) -> bytes:
    """
    Node hash compatible with OpenZeppelin MerkleProof sorted-pair assumption:
    keccak256(min(a,b) || max(a,b))
    """
    return keccak(a + b) if a <= b else keccak(b + a)


def build_layers(leaves: List[bytes]) -> List[List[bytes]]:
    """
    Build merkle layers, duplicating last node when odd at each level.
    layers[0] = leaves, layers[-1][0] = root
    """
    if not leaves:
        raise ValueError("No leaves (empty input).")

    layers = [leaves]
    while len(layers[-1]) > 1:
        cur = layers[-1]
        nxt: List[bytes] = []
        i = 0
        while i < len(cur):
            left = cur[i]
            right = cur[i + 1] if (i + 1) < len(cur) else cur[i]  # duplicate last if odd
            nxt.append(hash_pair(left, right))
            i += 2
        layers.append(nxt)
    return layers


def get_proof(layers: List[List[bytes]], leaf_index: int) -> List[str]:
    """
    Proof is list of sibling hashes (bytes32) from leaf level up to (but excluding) root.
    """
    proof: List[str] = []
    idx = leaf_index

    for level in range(len(layers) - 1):
        layer = layers[level]
        sibling_idx = idx ^ 1
        if sibling_idx < len(layer):
            sibling = layer[sibling_idx]
        else:
            sibling = layer[idx]  # duplicated last node case
        proof.append("0x" + sibling.hex())
        idx //= 2

    return proof


def usage() -> str:
    return (
        "Usage:\n"
        "  python generate_claims.py <input_csv> <output_json> <unit>\n\n"
        "Arguments (positional):\n"
        "  <input_csv>   Path to input CSV\n"
        "  <output_json> Output JSON path (e.g. claims.json)\n"
        "  <unit>        'eth' or 'wei' (unit of rewardTotal in CSV)\n\n"
        "CSV schema expected (header must include these columns):\n"
        "  wallet, ... , rewardTotal\n"
        "Script uses:\n"
        "  address column = 'wallet'\n"
        "  amount column  = 'rewardTotal'\n"
        "Note:\n"
        "  Rows with rewardTotal == 0 wei are skipped.\n"
    )


def main() -> None:
    # Direct sys.argv positional assignment (as requested)
    if len(sys.argv) != 4:
        print(usage(), file=sys.stderr)
        raise SystemExit(2)

    input_csv = sys.argv[1]
    output_json = sys.argv[2]
    unit = sys.argv[3].strip().lower()

    if unit not in ("eth", "wei"):
        print("Error: <unit> must be 'eth' or 'wei'\n", file=sys.stderr)
        print(usage(), file=sys.stderr)
        raise SystemExit(2)

    address_col = "wallet"
    amount_col = "rewardTotal"

    # Read CSV
    rows: List[Dict[str, str]] = []
    try:
        with open(input_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise SystemExit("Input CSV appears to have no header row.")
            for r in reader:
                rows.append(r)
    except FileNotFoundError:
        raise SystemExit(f"Input CSV not found: {input_csv!r}")

    if not rows:
        raise SystemExit("Input CSV has no data rows.")

    # Validate required columns
    header = set(rows[0].keys())
    if address_col not in header:
        raise SystemExit(f"Missing required column {address_col!r} in CSV header.")
    if amount_col not in header:
        raise SystemExit(f"Missing required column {amount_col!r} in CSV header.")

    # Normalize + prepare values
    # IMPORTANT CHANGE: skip rows with amount_wei == 0
    values: List[Tuple[int, str, int, Dict[str, Any]]] = []
    skipped_zero = 0

    for row_idx, r in enumerate(rows):
        addr_raw = (r.get(address_col) or "").strip()
        if not addr_raw:
            raise SystemExit(f"Row {row_idx+1}: empty wallet address.")

        try:
            account = to_checksum_address(addr_raw)
        except Exception as e:
            raise SystemExit(f"Row {row_idx+1}: invalid address {addr_raw!r}: {e}")

        amt_raw = (r.get(amount_col) or "").strip()
        if not amt_raw:
            raise SystemExit(f"Row {row_idx+1}: empty rewardTotal.")

        try:
            amount_wei = parse_amount_to_wei(amt_raw, unit)
        except Exception as e:
            raise SystemExit(f"Row {row_idx+1}: bad rewardTotal {amt_raw!r}: {e}")

        if amount_wei == 0:
            skipped_zero += 1
            continue

        meta = dict(r)  # keep original row for auditing/UI (optional)
        values.append((len(values), account, amount_wei, meta))
        # Note: index is now contiguous among included claims (0..n-1)

    if not values:
        raise SystemExit("All rows were skipped (no wallets with non-zero rewardTotal).")

    # Build leaves and merkle tree
    leaves = [hash_leaf(i, account, amount_wei) for (i, account, amount_wei, _meta) in values]
    layers = build_layers(leaves)
    root = layers[-1][0]
    root_hex = "0x" + root.hex()

    # Build claims JSON
    claims: Dict[str, Any] = {}
    for (i, account, amount_wei, meta) in values:
        proof = get_proof(layers, i)
        claim_obj: Dict[str, Any] = {
            "index": str(i),
            "amountWei": str(amount_wei),
            "proof": proof,
            "csv": meta,  # remove if you want smaller file
        }
        if unit == "eth":
            claim_obj["amountEth"] = str(Decimal(amount_wei) / WEI_PER_ETH)
        claims[account] = claim_obj

    out = {
        "merkleRoot": root_hex,
        "unit": "wei",
        "leafEncoding": ["address", "uint256"],
        "leafHash": "keccak256(bytes.concat(keccak256(abi.encode(account, amountWei))))",
        "nodeHash": "keccak256(min(a,b) || max(a,b))  // sorted-pair hash",
        "claims": claims,
        "stats": {
            "includedWallets": len(values),
            "skippedZeroAmountWallets": skipped_zero,
            "inputRows": len(rows),
        },
    }

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print("merkleRoot:", root_hex)
    print("input rows:", len(rows))
    print("included wallets:", len(values))
    print("skipped zero-amount wallets:", skipped_zero)
    print("wrote:", output_json)


if __name__ == "__main__":
    main()
