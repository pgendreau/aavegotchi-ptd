# Aavegotchi PTD - Combined VP
**P**artial **T**reasury **D**istribution

## Description
This script (getTotalVp.js) can be used to get a wallet's combined VP (Base VP + Polygon GHST + GotchiPockets Base + GotchiPockets Polygon) used to calculate the distribution for a given address.

## Prerequisites
1. Create a file named .env from .env.example containing the RPC provider url for each network.
   
   **Note**: The node need to be an archive node as we are querying blocks in the past.

2. install dependencies with `npm install`

3. Clone snapshot's [score-api](https://github.com/snapshot-labs/score-api) git repository

4. Cd into the score-api directory run `git reset --hard 356383eabcf17528f94bcef194a5f77179161f35`

5. Run `npm install`

6. Run a local copy of the score api using the docker-compose file in the root of the repository.
   `docker-compose up`

## Usage
`node getTotalVp.js <address>`

Example:

`node getTotalVp.js 0x6b175474e89094c44da98b954eedeac495271d0f`
