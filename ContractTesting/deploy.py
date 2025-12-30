# deploy.py
import json
from pathlib import Path

from web3 import Web3
from solcx import install_solc, compile_files

# ---------------------------
# HARD-CODED SETTINGS (EDIT)
# ---------------------------

RPC_URL = "YOUR BASE SEPOLIA RPC"  # Base Sepolia RPC
EXPECTED_CHAIN_ID = 84532          # Base Sepolia Chain ID

# Private key that will SIGN TRANSACTIONS in this script.
PRIVATE_KEY = "PRIVATE KEY"        # This should be an env variable, I'm just being lazy.

# Constructor parameters
DAO_ADDRESS = "DAO ADDRESS"
OWNER_ADDRESS = "OWNER ADDRESS"    # can be the same

# Paths
CONTRACT_PATH = "contracts/MerkleDistributor.sol"
CONTRACT_NAME = "MerkleDistributor"

# Must match your pragma: `pragma solidity 0.8.24;`
SOLC_VERSION = "0.8.24"


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


def compile_contract():
    install_solc(SOLC_VERSION)
    compiled = compile_files(
        [CONTRACT_PATH],
        output_values=["abi", "bin"],
        solc_version=SOLC_VERSION,
        import_remappings=["@openzeppelin/=node_modules/@openzeppelin/"],
        allow_paths=".,node_modules",
    )

    key = f"{CONTRACT_PATH}:{CONTRACT_NAME}"
    if key not in compiled:
        raise RuntimeError(f"Contract not found in compilation output. Keys: {list(compiled.keys())}")

    return compiled[key]["abi"], compiled[key]["bin"]


def send_tx(w3: Web3, account, tx: dict) -> str:
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    return tx_hash.hex()


def assert_rpc_ok(w3: Web3) -> int:
    try:
        chain_id = w3.eth.chain_id
        _ = w3.eth.block_number
    except Exception as e:
        raise SystemExit(f"RPC connection failed: {e}")

    if chain_id != EXPECTED_CHAIN_ID:
        raise SystemExit(f"Wrong chain_id {chain_id}. Expected {EXPECTED_CHAIN_ID} (Base Sepolia).")

    return chain_id


def main():
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 30}))
    chain_id = assert_rpc_ok(w3)

    acct = w3.eth.account.from_key(PRIVATE_KEY)
    signer = acct.address

    print(f"Connected to chain_id={chain_id}")
    print(f"Deployer address: {signer}")

    abi, bytecode = compile_contract()
    contract = w3.eth.contract(abi=abi, bytecode=bytecode)

    nonce = w3.eth.get_transaction_count(signer, "pending")
    print("Using nonce:", nonce)

    deploy_tx = contract.constructor(
        Web3.to_checksum_address(DAO_ADDRESS),
        Web3.to_checksum_address(OWNER_ADDRESS),
    ).build_transaction({
        "from": signer,
        "nonce": nonce,
        "chainId": chain_id,
        **eip1559_fees(w3),
    })

    gas_est = w3.eth.estimate_gas(deploy_tx)
    deploy_tx["gas"] = int(gas_est * 12 // 10)

    deploy_hash = send_tx(w3, acct, deploy_tx)
    print(f"Deploy tx: {deploy_hash}")

    receipt = w3.eth.wait_for_transaction_receipt(deploy_hash)
    if receipt.status != 1:
        raise SystemExit("Deployment failed (receipt.status != 1)")

    contract_address = receipt.contractAddress
    print(f"Deployed at: {contract_address}")

    out = {
        "chainId": chain_id,
        "rpcUrl": RPC_URL,
        "contractAddress": contract_address,
        "abi": abi,
        "dao": DAO_ADDRESS,
        "owner": OWNER_ADDRESS,
        "deployer": signer,
    }
    Path("deployment_output.json").write_text(json.dumps(out, indent=2))
    print("Wrote deployment_output.json")


if __name__ == "__main__":
    main()

