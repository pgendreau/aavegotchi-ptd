# Aavegotchi PTD - Rarity Farming based Pool

Calculates rewards for the RF pool of PTD.

## Description
Using the snapshot from Round 1 of RF S12 (as was voted for in poll PTD-F), CalculateRFRewards calculates the amount of ETH that an eligible wallet would receive from the PTD.

## Prerequisites
1. First run the 2-step VotingData pipeline to receive OutputWeights.csv (the weights are not considered here, but it includes all the eligible wallets).
2. BRS leaderboard data (leaderboard_withSetsRarityScore_block_37694538.csv)
3. KIN leaderboard data (leaderboard_kinship_block_37694538.csv)
4. XP leaderboard data (leaderboard_experience_block_37694538.csv)

## Usage:

  python CalculateRFRewards.py \ \
      VotingData/OutputWeights.csv \ \
      RF/leaderboard_withSetsRarityScore_block_37694538.csv \ \
      RF/leaderboard_kinship_block_37694538.csv \ \
      RF/leaderboard_experience_block_37694538.csv \ \
      320 \ \
      "0.625,0.25,0.125" \ \
      RF/OutputRFRewards.csv

Assuming a total distribution of 1200, 400 would be distributed via the RF pool.
The script does not consider Battler rewards, so distributing 400 via:
20% Battler,
50% BRS,
20% Kinship,
10% XP,
would be equivalent to distributing 320 via 62.5% BRS, 25% KIN, 12.5% XP.
