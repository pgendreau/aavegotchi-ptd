const { ethers } = require("ethers");
const snapshot = require('@snapshot-labs/snapshot.js');

require('dotenv').config({ quiet: true });

// --- Configuration ---
const snapshotConfig = {
  space: 'aavegotchi.eth',
  // base
  8453: {
    blockNumber: 37782588,
    url: "https://score.snapshot.org",
    subgraphUrl: "https://subgraph.satsuma-prod.com/tWYl5n5y04oz/aavegotchi/aavegotchi-core-base/api",
    providerURL: process.env.BASE_PROVIDER_URL,
    ghstAddress: "0xcD2F22236DD9Dfe2356D7C543161D4d260FD9BcB",
    strategies: [
      // GHST
      {
        "name": "erc20-balance-of",
        "params": {
          "symbol": "GHST",
          "address": "0xcd2f22236dd9dfe2356d7c543161d4d260fd9bcb",
          "decimals": 18
        }
      },
      // Aavegotchis and wearables
      {
        "name": "aavegotchi-agip",
        "params": {
          "symbol": "GOTCHI",
          "tokenAddress": "0xA99c4B08201F2913Db8D28e71d020c4298F29dBF"
        }
      },
      // Land parcels
      {
        "name": "aavegotchi-agip-17",
        "params": {
          "symbol": "REALM"
        }
      },
      // GLTR pools (staked)
      {
        "name": "aavegotchi-agip-37-gltr-staked-lp",
        "params": {
          "symbol": "GHST",
          "decimals": 18,
          "ghstAddress": "0xcD2F22236DD9Dfe2356D7C543161D4d260FD9BcB",
          "ghstFudPoolId": 0,
          "ghstKekPoolId": 3,
          "ghstFomoPoolId": 1,
          "ghstFudAddress": "0xeae2fB93e291C2eB69195851813DE24f97f1ce71",
          "ghstGltrPoolId": 5,
          "ghstKekAddress": "0x699B4eb36b95cDF62c74f6322AaA140E7958Dc9f",
          "ghstWethPoolId": 4,
          "ghstAlphaPoolId": 2,
          "ghstFomoAddress": "0x62ab7d558A011237F8a57ac0F97601A764e85b88",
          "ghstGltrAddress": "0xa83b31D701633b8EdCfba55B93dDBC202D8A4621",
          "ghstWethAddress": "0x0DFb9Cb66A18468850d6216fCc691aa20ad1e091",
          "ghstAlphaAddress": "0x0Ba2A49aedf9A409DBB0272db7CDF98aEb1E1837",
          "gltrStakingAddress": "0xaB449DcA14413a6ae0bcea9Ea210B57aCe280d2c"
        }
      }
    ]
  },
  // polygon
  137: {
    blockNumber: 78624868,
    url: "http://localhost:3003",
    subgraphUrl: "https://subgraph.satsuma-prod.com/tWYl5n5y04oz/aavegotchi/aavegotchi-core-matic/api",
    providerURL: process.env.POLYGON_PROVIDER_URL,
    ghstAddress: "0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7",
    strategies: [
      // GHST
      {
        "name": "erc20-balance-of",
        "params": {
          "symbol": "GHST",
          "address": "0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7",
          "decimals": 18
        }
      },
      // amGHST 
      {
        "name": "erc20-balance-of",
        "params": {
          "address": "0x080b5BF8f360F624628E0fb961F4e67c9e3c7CF1",
          "decimals": 18
        }
      },
      // wapGHST (staked/unstaked)
      {
        "name": "aavegotchi-agip-37-wap-ghst",
        "params": {
          "ghstAddress": "0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7",
          "gltrStakingAddress": "0x1fE64677Ab1397e20A1211AFae2758570fEa1B8c",
          "amGhstAddress": "0x080b5BF8f360F624628E0fb961F4e67c9e3c7CF1",
          "wapGhstAddress": "0x73958d46B7aA2bc94926d8a215Fa560A5CdCA3eA",
          "wapGhstPoolId": 0,
          "ghstFudAddress": "0xfec232cc6f0f3aeb2f81b2787a9bc9f6fc72ea5c",
          "ghstFudPoolId": 1,
          "ghstFomoAddress": "0x641ca8d96b01db1e14a5fba16bc1e5e508a45f2b",
          "ghstFomoPoolId": 2,
          "ghstAlphaAddress": "0xc765eca0ad3fd27779d36d18e32552bd7e26fd7b",
          "ghstAlphaPoolId": 3,
          "ghstKekAddress": "0xbfad162775ebfb9988db3f24ef28ca6bc2fb92f0",
          "ghstKekPoolId": 4,
          "ghstUsdcAddress": "0x096c5ccb33cfc5732bcd1f3195c13dbefc4c82f4",
          "ghstUsdcPoolId": 5,
          "ghstWmaticAddress": "0xf69e93771F11AECd8E554aA165C3Fe7fd811530c",
          "ghstWmaticPoolId": 6,
          "ghstGltrAddress": "0xb0E35478a389dD20050D66a67FB761678af99678",
          "ghstGltrPoolId": 7,
          "symbol": "GHST",
          "decimals": 18
        }
      },
      // GLTR pools (staked)
      {
        "name": "aavegotchi-agip-37-gltr-staked-lp",
        "params": {
          "ghstAddress": "0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7",
          "gltrStakingAddress": "0x1fE64677Ab1397e20A1211AFae2758570fEa1B8c",
          "amGhstAddress": "0x080b5BF8f360F624628E0fb961F4e67c9e3c7CF1",
          "wapGhstAddress": "0x73958d46B7aA2bc94926d8a215Fa560A5CdCA3eA",
          "wapGhstPoolId": 0,
          "ghstFudAddress": "0xfec232cc6f0f3aeb2f81b2787a9bc9f6fc72ea5c",
          "ghstFudPoolId": 1,
          "ghstFomoAddress": "0x641ca8d96b01db1e14a5fba16bc1e5e508a45f2b",
          "ghstFomoPoolId": 2,
          "ghstAlphaAddress": "0xc765eca0ad3fd27779d36d18e32552bd7e26fd7b",
          "ghstAlphaPoolId": 3,
          "ghstKekAddress": "0xbfad162775ebfb9988db3f24ef28ca6bc2fb92f0",
          "ghstKekPoolId": 4,
          "ghstUsdcAddress": "0x096c5ccb33cfc5732bcd1f3195c13dbefc4c82f4",
          "ghstUsdcPoolId": 5,
          "ghstWmaticAddress": "0xf69e93771F11AECd8E554aA165C3Fe7fd811530c",
          "ghstWmaticPoolId": 6,
          "ghstGltrAddress": "0xb0E35478a389dD20050D66a67FB761678af99678",
          "ghstGltrPoolId": 7,
          "symbol": "GHST",
          "decimals": 18
        }
      },
      // GLTR pools (unstaked)
      {
        "name": "contract-call",
        "params": {
          "address": "0x0D00800489dcAb402D4A17C5BAAfe80c4E22a5d9",
          "symbol": "AGIP37-Unstaked-LP",
          "decimals": 18,
          "methodABI": {
            "inputs": [
              {
                "internalType": "address",
                "name": "account",
                "type": "address"
              }
            ],
            "name": "gltrAllUnstakedLPTokenVotingPower",
            "outputs": [
              {
                "internalType": "uint256",
                "name": "",
                "type": "uint256"
              }
            ],
            "stateMutability": "view",
            "type": "function"
          }
        }
      },
      // FRENS pools
      {
        "name": "aavegotchi",
        "params": {
          "tokenAddress": "0x385Eeac5cB85A38A9a07A70c73e0a3271CfB54A7",
          "ghstQuickAddress": "0x8b1fd78ad67c7da09b682c5392b65ca7caa101b9",
          "ghstUsdcAddress": "0x096c5ccb33cfc5732bcd1f3195c13dbefc4c82f4",
          "ghstWethAddress": "0xccb9d2100037f1253e6c1682adf7dc9944498aff",
          "ghstWmaticAddress": "0xf69e93771F11AECd8E554aA165C3Fe7fd811530c",
          "stakingAddress": "0xA02d547512Bb90002807499F05495Fe9C4C3943f",
          "symbol": "GHST",
          "decimals": 18
        }
      }
    ]
  }
};

// Calculate voting power according to configured strategies
async function getVotingPower(address, network) {

  try {
    const score = await snapshot.utils.getVp(
      address,
      network,
      snapshotConfig[network].strategies,
      snapshotConfig[network].blockNumber,
      snapshotConfig.space,
      false,
      {
        url: snapshotConfig[network].url
      }
    );

    return score.vp;

  } catch (error) {
    console.error('Failed to fetch voting power:', error);
  }
};

// get all gotchi pocket addresses for a owner
async function getGotchiEscrows(address, network) {

  // subgraph requires lowercase addresses
  const owner = address.toLowerCase();

  const query = ` 
    query AavegotchisOfOwner {
      aavegotchis(
        block: { number: ${snapshotConfig[network].blockNumber} },
        where:  {
           originalOwner: "${owner}"
        }
      ) {
        escrow
      }
    }
  `;

  const response = await fetch(snapshotConfig[network].subgraphUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });

  if (!response.ok) {
    throw new Error(`Error retrieving subgraph data: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  return data.data.aavegotchis.map((gotchi) => gotchi.escrow);
};

// get GHST balance for a list of addresses
async function getTokenBalances(network, addresses) {
  const provider = new ethers.JsonRpcProvider(snapshotConfig[network].providerURL);
  const tokenContract = new ethers.Contract(snapshotConfig[network].ghstAddress, ["function balanceOf(address) view returns (uint256)"], provider);

  const tokenBalances = [];

  for (const address of addresses) {
    try {
      const balance = await tokenContract.balanceOf(address, { blockTag: snapshotConfig[network].blockNumber });
      tokenBalances.push(balance.toString());
    } catch (error) {
      console.error(`Error getting balance for address ${address}: ${error}`);
    }
  }

  return tokenBalances;
};

// --- Get the snapshot distribution number for an address ---
async function main() {
  // get address from command line
  if (process.argv.length < 3) {
    console.error('Please provide an address as a command line argument.');
    process.exit(1);
  }
  const address = process.argv[2];

  // define networks
  const baseChainId = 8453;
  const polygonChainId = 137;

  // get all gotchi pocket addresses
  const gotchiEscrowsBase = await getGotchiEscrows(address, baseChainId);
  const gotchiEscrowsPolygon = await getGotchiEscrows(address, polygonChainId);
  // get all gotchi pocket balances
  const pocketBalancesBase = await getTokenBalances(baseChainId, gotchiEscrowsBase);
  const pocketBalancesPolygon = await getTokenBalances(polygonChainId, gotchiEscrowsPolygon);
  // get total balance for each network in wei
  const totalPocketsBalanceBaseWei = pocketBalancesBase.reduce((accumulator, currentValue) => {
    return BigInt(accumulator) + BigInt(currentValue); 
  }, 0n);
  const totalPocketsBalancePolygonWei = pocketBalancesPolygon.reduce((accumulator, currentValue) => {
    return BigInt(accumulator) + BigInt(currentValue); 
  }, 0n);
  // convert to GHST
  const totalPocketsBalanceBase = ethers.formatEther(totalPocketsBalanceBaseWei);
  const totalPocketsBalancePolygon = ethers.formatEther(totalPocketsBalancePolygonWei);

  // get voting power for each network
  const baseVp = await getVotingPower(address, baseChainId);
  const polygonVp = await getVotingPower(address, polygonChainId);

  // get total voting power
  const totalVp = baseVp + polygonVp;
  // get total gotchi pocket balance
  const totalPocketsBalance = Number(totalPocketsBalanceBase) + Number(totalPocketsBalancePolygon);

  console.log("Base VP:", baseVp);
  console.log("Polygon VP:", polygonVp);
  console.log("Base Pockets Balance:", Number(totalPocketsBalanceBase));
  console.log("Polygon Pockets Balance:", Number(totalPocketsBalancePolygon));
  
  // get total number for distribtion
  console.log("Total:", totalVp + totalPocketsBalance);
};

main().catch((error) => {
  console.error(error);
});
