# fund.py
import json
from pathlib import Path

from web3 import Web3

# ---------------------------
# HARD-CODED SETTINGS (EDIT)
# ---------------------------

RPC_URL = "YOUR BASE SEPOLIA RPC"  # Base Sepolia RPC
EXPECTED_CHAIN_ID = 84532          # Base Sepolia Chain ID

# IMPORTANT: This must be the DAO key because openClaims() is onlyDAO.
DAO_PRIVATE_KEY = "PRIVATE KEY"    # This should be an env variable, I'm just being lazy.

# Your merkle/claims file (you said you have ./claims.json)
MERKLE_JSON_PATH = "../claims.json"

# Read deployment_output.json written by deploy_only.py (or manually create it)
DEPLOYMENT_INFO_PATH = "deployment_output.json"


def eip1559_fees(w3: Web3) -> dict:
    latest = w3.eth.get_block("latest")
    base_fee = latest.get("baseFeePerGas", 0)

    max_priority = w3.to_wei(0.01, "gwei")
    if base_fee:
        max_fee = base_fee * 2 + max_priority
    else:
        max_fee = w3.to_wei(0.1, "gwei")

    return {
        "type": 2,
        "maxPriorityFeePerGas": int(max_priority),
        "maxFeePerGas": int(max_fee),
    }


def assert_rpc_ok(w3: Web3) -> int:
    try:
        chain_id = w3.eth.chain_id
        _ = w3.eth.block_number
    except Exception as e:
        raise SystemExit(f"RPC connection failed: {e}")

    if chain_id != EXPECTED_CHAIN_ID:
        raise SystemExit(f"Wrong chain_id {chain_id}. Expected {EXPECTED_CHAIN_ID} (Base Sepolia).")

    return chain_id


def load_merkle_data(path: str) -> tuple[str, int]:
    data = json.loads(Path(path).read_text())
    merkle_root = data["merkleRoot"]

    claims = data.get("claims", {})
    if not claims:
        raise SystemExit("No claims found in JSON. claims{} is empty.")

    total = 0
    for addr, entry in claims.items():
        amt = entry.get("amountWei")
        if amt is None:
            raise ValueError(f"Missing amountWei for {addr}")
        total += int(amt)

    return merkle_root, total


def send_tx(w3: Web3, account, tx: dict) -> str:
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    return tx_hash.hex()


def main():
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
    chain_id = assert_rpc_ok(w3)

    deploy_info = json.loads(Path(DEPLOYMENT_INFO_PATH).read_text())
    contract_address = Web3.to_checksum_address(deploy_info["contractAddress"])
    abi = deploy_info["abi"]
    dao_expected = Web3.to_checksum_address(deploy_info["dao"])

    dao_acct = w3.eth.account.from_key(DAO_PRIVATE_KEY)
    dao_signer = Web3.to_checksum_address(dao_acct.address)

    print(f"Connected to chain_id={chain_id}")
    print(f"Contract address: {contract_address}")
    print(f"DAO (from deployment_output.json): {dao_expected}")
    print(f"DAO signer (from DAO_PRIVATE_KEY): {dao_signer}")

    if dao_signer != dao_expected:
        raise SystemExit(
            "DAO_PRIVATE_KEY does not match the DAO address stored in deployment_output.json.\n"
            "Use the correct DAO key or update the deployment info."
        )

    merkle_root, total_wei = load_merkle_data(MERKLE_JSON_PATH)
    print(f"Merkle root: {merkle_root}")
    print(f"Total distribution (wei): {total_wei}")
    print(f"Total distribution (ETH): {w3.from_wei(total_wei, 'ether')}")

    balance = w3.eth.get_balance(dao_signer)
    print(f"DAO balance (ETH): {w3.from_wei(balance, 'ether')}")
    # Leave some headroom for gas; this is a simple safety check.
    if balance <= total_wei:
        raise SystemExit("DAO balance is not enough to cover the deposit (and gas). Top up testnet ETH.")

    deployed = w3.eth.contract(address=contract_address, abi=abi)

    # # Optional: prevent wasting gas if claims already opened
    # current_root = deployed.functions.merkleRoot().call()
    # if int(current_root, 16) != 0:
    #     raise SystemExit(f"Claims already opened. merkleRoot is already set: {current_root}")

    current_root = deployed.functions.merkleRoot().call()

    # current_root is bytes (length 32). Zero root means claims not opened.
    if current_root != b"\x00" * 32:
        # Print as hex for readability
        raise SystemExit(f"Claims already opened. merkleRoot is already set: 0x{current_root.hex()}")

    nonce = w3.eth.get_transaction_count(dao_signer, "pending")
    print("Using nonce:", nonce)

    open_tx = deployed.functions.openClaims(merkle_root).build_transaction({
        "from": dao_signer,
        "nonce": nonce,
        "chainId": chain_id,
        "value": int(total_wei),
        **eip1559_fees(w3),
    })

    gas_est = w3.eth.estimate_gas(open_tx)
    open_tx["gas"] = int(gas_est * 12 // 10)

    tx_hash = send_tx(w3, dao_acct, open_tx)
    print(f"openClaims tx: {tx_hash}")

    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    if receipt.status != 1:
        raise SystemExit("openClaims failed (receipt.status != 1)")

    print("Claims are opened and funded.")


if __name__ == "__main__":
    main()
