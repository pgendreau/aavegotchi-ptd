# Aavegotchi PTD

**P**artial **T**reasury **D**istribution participation calculator

## Description
These scripts can be used to calculate the PTD distribution amounts for eligible wallets. The TotalDistributionAmounts.csv assumes a total distribution amount of 1200. The values can still change based on wallets becoming eligible by voting on the PTD AGIP, as well as by wallet-linking, which would be handled via typeform.

## Execution
To get the PTD distribution amounts for all eligible wallets, run the following scripts in order:

1. **VotingData/CalculateCachedVoteCounts.py**  
   This will fetch data from Snapshot for concluded proposals to create a cache.

2. **VotingData/CalculateVotingWeights.py**  
   This will fetch new data (e.g. proposals that are still active) and use the new data in combination with the generated cache to determine all eligible wallets as well as their voting percentages.

3. **CombinedVP/getTotalVp.js**  
   This will get a wallet's combined VP. Run it for every wallet in VotingData/EligibleWalletsLatest.csv to generate CombinedVP.csv.

4. **RF/CalculateRFRewards.py**  
   This will calculate the distribution amounts for the RF-based pool for all eligible wallets.

5. **GetTotalDistributionAmounts.py**  
   This will calculate the current distribution amounts for all eligible wallets.
