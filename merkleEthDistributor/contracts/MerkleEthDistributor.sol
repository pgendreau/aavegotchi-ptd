// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/utils/cryptography/MerkleProof.sol";
import "@openzeppelin/contracts/utils/ReentrancyGuard.sol";

contract MerkleEthDistributor is ReentrancyGuard {
    bytes32 public merkleRoot;
    address public owner;
    uint64 public startTime;
    uint64 public endTime;

    bool private _initialized;
    mapping(uint256 => bool) public claimed; // index => claimed

    event Initialized(address indexed owner, bytes32 merkleRoot, uint64 startTime, uint64 endTime);
    event Claimed(uint256 indexed index, address indexed account, uint256 amountWei);
    event RootUpdated(bytes32 oldRoot, bytes32 newRoot);
    event Swept(address indexed to, uint256 amountWei);

    modifier onlyOwner() {
        require(msg.sender == owner, "NOTOWNER");
        _;
    }

    function initialize(
        address _owner,
        bytes32 _merkleRoot,
        uint64 _startTime,
        uint64 _endTime
    ) external {
        require(!_initialized, "ALREADY_INIT");
        _initialized = true;

        owner = _owner;
        merkleRoot = _merkleRoot;
        startTime = _startTime;
        endTime = _endTime;

        emit Initialized(owner, merkleRoot, startTime, endTime);
    }

    receive() external payable {}
function claim(
        uint256 index,
        address account,
        uint256 amountWei,
        bytes32[] calldata merkleProof
    ) external nonReentrant {
        require(block.timestamp >= startTime, "NOT_STARTED");
        if (endTime != 0) require(block.timestamp <= endTime, "ENDED");
        require(msg.sender == account, "SENDER_NOT_ACCOUNT");
        require(!claimed[index], "ALREADY_CLAIMED");

        bytes32 leaf = keccak256(abi.encode(index, account, amountWei));
        require(MerkleProof.verify(merkleProof, merkleRoot, leaf), "BAD_PROOF");

        claimed[index] = true;

        (bool ok, ) = account.call{value: amountWei}("");
        require(ok, "ETH_SEND_FAIL");

        emit Claimed(index, account, amountWei);
    }

    // Optional: if you want the DAO to be able to change the list later (often you DON'T)
    function setMerkleRoot(bytes32 newRoot) external onlyOwner {
        bytes32 old = merkleRoot;
        merkleRoot = newRoot;
        emit RootUpdated(old, newRoot);
    }

    function sweep(address to) external onlyOwner nonReentrant {
        if (endTime != 0) require(block.timestamp > endTime, "NOT_ENDED");
        uint256 bal = address(this).balance;
        (bool ok, ) = to.call{value: bal}("");
        require(ok, "SWEEP_FAIL");
        emit Swept(to, bal);
    }
}
