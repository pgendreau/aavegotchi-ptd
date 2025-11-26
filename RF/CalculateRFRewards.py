#!/usr/bin/env python3
"""
CalculateRFRewards.py

Overview
========
This script computes a reward distribution across *eligible wallets* (addresses) using
three ranked leaderboards:

  1) BRSRanking.csv  (rarity / BRS leaderboard)
  2) KINRankings.csv (kinship leaderboard)
  3) XPRankings.csv  (experience / XP leaderboard)

We treat each row in each leaderboard as an "entry" that belongs to an owner wallet
(the `originalOwner` field). Each entry receives a weight based on its leaderboard
`position` using a power-law formula (different exponent per leaderboard). After
filtering to eligible owners, these entry-weights are normalized so that the sum of
weights in each leaderboard equals 1.0.

Then we allocate rewards:
- You specify a total RewardAmount (float) to distribute.
- You specify RewardPercentages for the three leaderboards (BRS, KIN, XP).
  Example: "50,30,20" meaning:
      50% of RewardAmount goes to BRS leaderboard,
      30% to KIN leaderboard,
      20% to XP leaderboard.

Within each leaderboard:
- Each eligible entry gets:
      entry_reward = leaderboard_pool * normalized_entry_weight
- An owner's reward from that leaderboard is the sum of entry_rewards over all
  entries belonging to that owner.
- The final reward for an owner is:
      total_reward(owner) = reward_BRS(owner) + reward_KIN(owner) + reward_XP(owner)

Eligibility
===========
Eligible wallets are taken from OutputWeights.csv (from the previous pipeline step).
Schema:
    wallet,num_proposals,weight
We only use the 'wallet' column. These are the addresses that may receive rewards.

Leaderboards
============
Each leaderboard CSV has schema:
position,gotchiID,formattedTokenID,name,withSetsRarityScore,kinship,experience,level,hauntId,owner,originalOwner,status,stakedAmount

We only use:
  - position        (rank, 1 = best)
  - originalOwner   (wallet address to reward)
All addresses are normalized to lowercase for matching.

Weight formulas (per entry)
===========================
For a row with rank position p (p must be positive):
  - BRS: (1.0 / p) ** 0.94
  - KIN: (1.0 / p) ** 0.76
  - XP : (1.0 / p) ** 0.65

Processing steps (per leaderboard)
==================================
1) Load the leaderboard CSV.
2) Normalize `originalOwner` to lowercase.
3) Filter out rows whose originalOwner is NOT in the eligible wallet set.
4) Compute an unnormalized weight for each remaining row using its `position`.
5) Sum all unnormalized weights. If the sum is 0 or there are no rows, the leaderboard contributes 0.
6) Normalize each entry weight by dividing by the sum, so the normalized weights sum to 1.0.
7) Convert entry weights to entry rewards by multiplying with that leaderboard's pool.
8) Aggregate entry rewards by originalOwner (wallet).

Output
======
Writes a CSV file containing all eligible wallets and their combined reward amount.

Output schema:
    wallet,reward

Notes:
- Eligible wallets that have no entries in any leaderboard will receive reward = 0.
- You can optionally also add diagnostic prints for totals and missing data.

Usage
=====
    python CalculateRFRewards.py \
        OutputWeights.csv \
        BRSRanking.csv KINRankings.csv XPRankings.csv \
        RewardAmount \
        RewardPercentages \
        OutputRewards.csv

Where:
  - RewardAmount is a float, e.g. 12345.67
  - RewardPercentages is a comma-separated list or 3 args. Supported formats:
        "50,30,20"  (percentages summing to 100)
    or  "0.5,0.3,0.2" (fractions summing to 1)
    or  "50 30 20" (if your shell splits args; see below)

Example:
  python CalculateRFRewards.py \
      VotingData/OutputWeights.csv \
      RF/leaderboard_withSetsRarityScore_block_37694538.csv \
      RF/leaderboard_kinship_block_37694538.csv \
      RF/leaderboard_experience_block_37694538.csv \
      320 \
      "0.625,0.25,0.125" \
      RF/OutputRFRewards.csv

"""

import csv
import sys
from typing import Dict, Iterable, List, Set, Tuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_addr(addr: str) -> str:
    """Strip whitespace and lowercase wallet addresses for consistent matching."""
    if not addr:
        return ""
    return addr.strip().lower()


def parse_percentages(s: str) -> Tuple[float, float, float]:
    """
    Parse RewardPercentages.

    Accepts:
      - "50,30,20" (sums to 100-ish)   -> converted to fractions
      - "0.5,0.3,0.2" (sums to 1-ish)  -> used directly as fractions

    Returns:
      (brs_frac, kin_frac, xp_frac) as floats that sum to 1.0.

    Raises ValueError if parsing fails or sums are invalid.
    """
    parts = [p.strip() for p in s.replace(";", ",").split(",") if p.strip()]
    if len(parts) != 3:
        raise ValueError("RewardPercentages must have exactly 3 values: BRS,KIN,XP (e.g. '50,30,20').")

    vals = [float(p) for p in parts]
    total = sum(vals)

    if total <= 0:
        raise ValueError("RewardPercentages sum must be > 0.")

    # If they look like percentages (sum ~100), convert to fractions.
    if 99.0 <= total <= 101.0:
        vals = [v / 100.0 for v in vals]
        total = sum(vals)

    # Now require sum ~ 1.
    if not (0.99 <= total <= 1.01):
        raise ValueError(
            f"RewardPercentages must sum to 1.0 (fractions) or 100 (percent). Got sum={total}."
        )

    # Normalize precisely to 1.0 to avoid drift.
    vals = [v / total for v in vals]
    return vals[0], vals[1], vals[2]


def load_eligible_wallets_from_outputweights(path: str) -> Set[str]:
    """
    Load eligible wallets from OutputWeights.csv (schema: wallet,num_proposals,weight).
    We only use the 'wallet' column.

    Returns:
      set of normalized wallet addresses
    """
    eligible: Set[str] = set()
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "wallet" not in (reader.fieldnames or []):
            raise ValueError(f"{path} must have a 'wallet' column.")
        for row in reader:
            w = normalize_addr(row.get("wallet") or "")
            if w:
                eligible.add(w)
    return eligible


def iter_rank_entries(path: str) -> Iterable[Tuple[int, str]]:
    """
    Yield (position, originalOwner) from a ranking CSV.

    Required columns:
      - position
      - originalOwner
    """
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = set(reader.fieldnames or [])
        if "position" not in cols or "originalOwner" not in cols:
            raise ValueError(f"{path} must have columns 'position' and 'originalOwner'.")

        for row in reader:
            pos_raw = (row.get("position") or "").strip()
            owner_raw = row.get("originalOwner") or ""
            owner = normalize_addr(owner_raw)

            if not pos_raw or not owner:
                continue

            try:
                pos = int(float(pos_raw))
            except Exception:
                continue

            if pos <= 0:
                continue

            yield pos, owner


def compute_owner_rewards_from_ranking(
    ranking_path: str,
    eligible_wallets: Set[str],
    pool_amount: float,
    exponent: float,
) -> Dict[str, float]:
    """
    Compute per-owner reward allocation for one ranking list.

    Steps:
      - Filter rows to eligible owners.
      - Compute entry weights: (1/position)^exponent
      - Normalize entry weights to sum to 1.0
      - Multiply by pool_amount to get entry rewards
      - Sum entry rewards by owner

    Returns:
      dict owner -> reward_from_this_ranking
    """
    # Collect (owner, w_i) for eligible entries
    entries: List[Tuple[str, float]] = []
    for pos, owner in iter_rank_entries(ranking_path):
        if owner not in eligible_wallets:
            continue
        w = (1.0 / float(pos)) ** exponent
        if w > 0:
            entries.append((owner, w))

    if not entries or pool_amount <= 0:
        return {}

    total_w = sum(w for _, w in entries)
    if total_w <= 0:
        return {}

    # Normalize and assign rewards
    rewards: Dict[str, float] = {}
    for owner, w in entries:
        share = w / total_w
        rewards[owner] = rewards.get(owner, 0.0) + share * pool_amount

    return rewards


def write_rewards_output(
    output_path: str,
    eligible_wallets: Set[str],
    total_rewards: Dict[str, float],
) -> None:
    """
    Write output CSV:
      wallet,reward

    Includes ALL eligible wallets, even if reward=0.
    Sorted by reward desc then wallet asc for determinism.
    """
    rows: List[Tuple[str, float]] = []
    for w in eligible_wallets:
        rows.append((w, float(total_rewards.get(w, 0.0))))

    rows.sort(key=lambda x: (-x[1], x[0]))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "reward"])
        for w, r in rows:
            writer.writerow([w, f"{r:.10f}"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 8:
        print(
            "Usage:\n"
            "  python CalculateRFRewards.py "
            "OutputWeights.csv BRSRanking.csv KINRankings.csv XPRankings.csv RewardAmount RewardPercentages OutputRewards.csv\n\n"
            "RewardPercentages examples: \"50,30,20\" or \"0.5,0.3,0.2\"",
            file=sys.stderr,
        )
        sys.exit(1)

    outputweights_path = sys.argv[1]
    brs_path = sys.argv[2]
    kin_path = sys.argv[3]
    xp_path = sys.argv[4]
    reward_amount_raw = sys.argv[5]
    reward_percentages_raw = sys.argv[6]
    output_path = sys.argv[7]

    eligible_wallets = load_eligible_wallets_from_outputweights(outputweights_path)
    if not eligible_wallets:
        print("No eligible wallets found in OutputWeights.csv; output will be empty rewards.", file=sys.stderr)

    try:
        reward_amount = float(reward_amount_raw)
    except Exception:
        raise ValueError(f"RewardAmount must be a float. Got: {reward_amount_raw}")

    if reward_amount < 0:
        raise ValueError("RewardAmount must be >= 0.")

    brs_frac, kin_frac, xp_frac = parse_percentages(reward_percentages_raw)

    brs_pool = reward_amount * brs_frac
    kin_pool = reward_amount * kin_frac
    xp_pool = reward_amount * xp_frac

    # Compute per-owner rewards for each ranking
    brs_rewards = compute_owner_rewards_from_ranking(
        ranking_path=brs_path,
        eligible_wallets=eligible_wallets,
        pool_amount=brs_pool,
        exponent=0.94,
    )
    kin_rewards = compute_owner_rewards_from_ranking(
        ranking_path=kin_path,
        eligible_wallets=eligible_wallets,
        pool_amount=kin_pool,
        exponent=0.76,
    )
    xp_rewards = compute_owner_rewards_from_ranking(
        ranking_path=xp_path,
        eligible_wallets=eligible_wallets,
        pool_amount=xp_pool,
        exponent=0.65,
    )

    # Combine rewards
    total_rewards: Dict[str, float] = {}
    for d in (brs_rewards, kin_rewards, xp_rewards):
        for owner, amt in d.items():
            total_rewards[owner] = total_rewards.get(owner, 0.0) + float(amt)

    # Write output
    write_rewards_output(output_path, eligible_wallets, total_rewards)

    # Optional diagnostics to stderr
    total_out = sum(total_rewards.get(w, 0.0) for w in eligible_wallets)
    print(f"Eligible wallets: {len(eligible_wallets)}", file=sys.stderr)
    print(f"RewardAmount: {reward_amount}", file=sys.stderr)
    print(f"Pools -> BRS: {brs_pool}, KIN: {kin_pool}, XP: {xp_pool}", file=sys.stderr)
    print(f"Total rewards assigned (eligible wallets): {total_out}", file=sys.stderr)
    print(f"Wrote: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
