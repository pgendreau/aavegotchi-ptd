# [AGIP 155] A Partial Treasury Distribution (PTD)

## Execution Instructions
These are the execution instructions according to Resolutions A.1 and K.2 of AGIP 155.

### 1. Contract address of the distribution smart contract deployed on Base
**0xf50326e1A6c6949Cc390c4EFE8ea538e29A4fA11**

The source code is publicly available and verified on BaseScan:
https://basescan.org/address/0xf50326e1a6c6949cc390c4efe8ea538e29a4fa11#code

### 2. The Merkle root for the distribution
Merkle root:  
**0xe07775359b09f65b178a4d87b31a37a0c67b6bdee74fe620edc57b3e7d87208b**  
  
Amount of ETH in wei:  
**1036920000000700000000**

In accordance with Resolution K.2, the above Merkle root and amount match between the following two sources, which were derived independently:  
https://github.com/pgendreau/aavegotchi-ptd/blob/main/claims.json  
derived from:  
https://github.com/pgendreau/aavegotchi-ptd/blob/main/GenerateClaims.py  
as well as  
https://github.com/pgendreau/distributor/blob/main/proofs-ptd-final.json  
derived from:  
https://github.com/pgendreau/distributor/blob/main/scripts/generateTree.ts

### 3. Execution

The multisig signers are instructed to call the function `createDistributor` (0x59206906) of the smart contract 0xf50326e1A6c6949Cc390c4EFE8ea538e29A4fA11 using the DAO wallet 0x62DE034b1A69eF853c9d0D8a33D26DF5cF26682E with the following parameters:  
**_merkleRoot (bytes32):**  
0xe07775359b09f65b178a4d87b31a37a0c67b6bdee74fe620edc57b3e7d87208b

**msg.Value (payable amount in wei):**  
1036920000000700000000

The DAO and the owner of the contract have already been set to 0x62DE034b1A69eF853c9d0D8a33D26DF5cF26682E, as instructed by the DAO's treasurer.  
The contract ABI can be found here:  
https://basescan.org/address/0xf50326e1a6c6949cc390c4efe8ea538e29a4fa11#code