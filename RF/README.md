# Aavegotchi PTD - Rarity Farming based Pool

Calculates rewards for the RF pool of PTD.

## Description
Using the snapshot from Round 1 of RF S12 (as voted for in poll PTD-F), CalculateRFRewards determines the amount that an eligible wallet would receive from the PTD.

## Prerequisites
1. First run the 2-step VotingData pipeline to generate EligibleWalletsLatest.csv
2. BRS leaderboard data (leaderboard_withSetsRarityScore_block_37694538.csv)
3. KIN leaderboard data (leaderboard_kinship_block_37694538.csv)
4. XP leaderboard data (leaderboard_experience_block_37694538.csv)

## Usage

  python CalculateRFRewards.py \ \
      ../VotingData/EligibleWalletsLatest.csv \ \
      leaderboard_withSetsRarityScore_block_37694538.csv \ \
      leaderboard_kinship_block_37694538.csv \ \
      leaderboard_experience_block_37694538.csv \ \
      296.26 \ \
      "0.625,0.25,0.125" \ \
      OutputRFRewards.csv

Assuming a total distribution of 1111, 370.333 would be distributed via the RF pool.  
The script does not consider Battler rewards, so distributing 370.333 via:  
20% Battler,  
50% BRS,  
20% Kinship,
10% XP,  
would be equivalent to distributing 296.26 via 62.5% BRS, 25% KIN, 12.5% XP.