# Aavegotchi PTD - Voting Data

Calculates voting counts and percentages for each wallet voting on eligible Snapshot proposals.

## Description
To avoid overloading the Snapshot API with tons of requests, calculations were split into a 2-step pipeline:
1. Concluded proposals are processed and cached.  
2. Cache and up-to-date data on new/active proposals are fetched in an online fashion.

## Prerequisites for calculating the cache
1. IncludedProposals.csv (a list of signal and core proposals resulting in passed AGIPs, excluding proposals for which Snapshot no longer provides voting data)  
2. AGIP6M.csv (a list of AGIPs - subset of IncludedProposals - from the past 6 months, used to determine wallet eligibility based on the PTD-D poll)  
3. GV2AV.csv (proposal IDs for eligible proposals that were mirrored on GotchiVault)  
4. WalletAliases.csv (a list of linked wallets, in case someone switched wallets - e.g., due to compromise)

## Prerequisites for calculating the current weights
1. Output from the caching step:  
   1.1 VoteCounts.csv (all wallets that voted on any proposals in IncludedProposals.csv and how many proposals they voted on)  
   1.2 EligibleWalletsCached.csv (a list of eligible wallets based on the cached data)  
   1.3 ConcludedDecisionCount.txt (number of proposals processed in the caching step)  
2. ActiveProposals.csv (new proposals - e.g., active ones - not included in the caching step)  
3. AGIPActive.csv (new AGIPs not included in the caching step; a subset of ActiveProposals)  
4. WalletAliases.csv (a list of linked wallets, in case someone switched wallets - e.g., due to compromise)

## Usage (cache):

Either use VoteCounts.csv, EligibleWalletsCached.csv, and ConcludedDecisionCount.txt from this repository, or calculate them yourself using:

python CalculateCachedVoteCounts.py IncludedProposals.csv AGIP6M.csv GV2AV.csv WalletAliases.csv VoteCounts.csv EligibleWalletsCached.csv ConcludedDecisionCount.txt

## Usage (weights):

Use the command:

python CalculateVotingWeights.py VoteCounts.csv EligibleWalletsCached.csv ConcludedDecisionCount.txt ActiveProposals.csv AGIPActive.csv WalletAliases.csv OutputWeights.csv EligibleWalletsLatest.csv
