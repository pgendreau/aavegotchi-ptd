// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/proxy/Clones.sol";
import "./MerkleEthDistributor.sol";

contract MerkleEthDistributorFactory {
    address public immutable implementation;

    event DistributorCreated(address indexed distributor, address indexed owner, bytes32 merkleRoot);

    constructor(address _implementation) {
        implementation = _implementation;
    }

    // payable so Safe can create+fund in one transaction (msg.value forwarded to the clone)
    function createDistributor(
        address owner,
        bytes32 merkleRoot,
        uint64 startTime,
        uint64 endTime,
        bytes32 salt
    ) external payable returns (address distributor) {
        distributor = Clones.cloneDeterministic(implementation, salt);

        MerkleEthDistributor(payable(distributor)).initialize(owner, merkleRoot, startTime, endTime);

        if (msg.value > 0) {
            (bool ok, ) = distributor.call{value: msg.value}("");
            require(ok, "FUNDFAIL");
        }

        emit DistributorCreated(distributor, owner, merkleRoot);
    }

    function predictDistributor(bytes32 salt) external view returns (address) {
        return Clones.predictDeterministicAddress(implementation, salt, address(this));
    }
}
