# reclaim.py
#
# Calls withdrawRemaining() on an already-deployed MerkleDistributor.
# Requirements:
# - deployment_output.json exists (written by your deploy script) OR set CONTRACT_ADDRESS and DAO_ADDRESS below
# - DAO private key is provided (withdrawRemaining is onlyDAO)
#
# Run:
#   python withdraw_remaining.py

import json
from pathlib import Path
from web3 import Web3

# ---------------------------
# HARD-CODED SETTINGS (EDIT)
# ---------------------------

RPC_URL = "YOUR BASE SEPOLIA RPC"  # Base Sepolia RPC
EXPECTED_CHAIN_ID = 84532          # Base Sepolia Chain ID

DAO_PRIVATE_KEY = "PRIVATE KEY"    # This should be an env variable, I'm just being lazy.

# If you have deployment_output.json, leave these as None.
# Otherwise set them explicitly.
DEPLOYMENT_INFO_PATH = "deployment_output.json"
CONTRACT_ADDRESS_OVERRIDE = None
DAO_ADDRESS_OVERRIDE = None


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


def load_deployment_info():
    if CONTRACT_ADDRESS_OVERRIDE and DAO_ADDRESS_OVERRIDE:
        return {
            "contractAddress": CONTRACT_ADDRESS_OVERRIDE,
            "dao": DAO_ADDRESS_OVERRIDE,
            "abi": json.loads(Path("deployment_output.json").read_text())["abi"]
            if Path("deployment_output.json").exists()
            else None,
        }

    data = json.loads(Path(DEPLOYMENT_INFO_PATH).read_text())
    return data


def send_tx(w3: Web3, account, tx: dict) -> str:
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    return tx_hash.hex()


def main():
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
    chain_id = assert_rpc_ok(w3)

    deploy_info = load_deployment_info()
    if "abi" not in deploy_info or deploy_info["abi"] is None:
        raise SystemExit(
            "ABI not found. Ensure deployment_output.json exists with an 'abi' field, "
            "or add ABI loading logic here."
        )

    contract_address = Web3.to_checksum_address(deploy_info["contractAddress"])
    abi = deploy_info["abi"]
    dao_expected = Web3.to_checksum_address(deploy_info["dao"])

    dao_acct = w3.eth.account.from_key(DAO_PRIVATE_KEY)
    dao_signer = Web3.to_checksum_address(dao_acct.address)

    print(f"Connected to chain_id={chain_id}")
    print(f"Contract address: {contract_address}")
    print(f"DAO (expected): {dao_expected}")
    print(f"DAO signer: {dao_signer}")

    if dao_signer != dao_expected:
        raise SystemExit("DAO_PRIVATE_KEY does not match the DAO address for this contract.")

    deployed = w3.eth.contract(address=contract_address, abi=abi)

    # Preflight checks to avoid wasting gas:
    claim_start = deployed.functions.claimStartTime().call()
    if claim_start == 0:
        raise SystemExit("ClaimsNotOpened: claimStartTime == 0 (openClaims was never called).")

    paused = deployed.functions.paused().call()
    period = deployed.functions.CLAIM_PERIOD().call()
    now = w3.eth.get_block("latest")["timestamp"]
    end_time = claim_start + period

    print(f"Paused: {paused}")
    print(f"Claim start: {claim_start}")
    print(f"Claim period (sec): {period}")
    print(f"Now: {now}")
    print(f"Claim end: {end_time}")

    if (not paused) and (now < end_time):
        raise SystemExit(
            "ClaimPeriodNotExpired: contract is not paused and claim period has not ended yet.\n"
            "Either wait until the end time, or pause the contract (owner/DAO) if you intend emergency withdrawal."
        )

    contract_balance = w3.eth.get_balance(contract_address)
    print(f"Contract balance (ETH): {w3.from_wei(contract_balance, 'ether')}")
    if contract_balance == 0:
        raise SystemExit("Contract balance is 0; nothing to withdraw.")

    nonce = w3.eth.get_transaction_count(dao_signer, "pending")
    print("Using nonce:", nonce)

    tx = deployed.functions.withdrawRemaining().build_transaction({
        "from": dao_signer,
        "nonce": nonce,
        "chainId": chain_id,
        **eip1559_fees(w3),
    })

    gas_est = w3.eth.estimate_gas(tx)
    tx["gas"] = int(gas_est * 12 // 10)

    tx_hash = send_tx(w3, dao_acct, tx)
    print(f"withdrawRemaining tx: {tx_hash}")

    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
    if receipt.status != 1:
        raise SystemExit("withdrawRemaining failed (receipt.status != 1)")

    print("Withdraw successful.")


if __name__ == "__main__":
    main()
