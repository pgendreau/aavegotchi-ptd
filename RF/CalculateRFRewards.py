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
Eligible wallets are taken from EligibleWalletsLatest.csv (from the previous pipeline step).
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

Additional per-entry outputs
============================
In addition to the per-wallet reward summary (OutputRewards.csv), this script now also writes
three *per-entry* CSV files, one per leaderboard:

  - BRSRewards.csv
  - KINRewards.csv
  - XPRewards.csv

Each of these files has the following schema:
    position,gotchiID,name,withSetsRarityScore,kinship,experience,level,originalOwner,
    originalWeight,weightAmongEligible,reward

Where:
  - position,gotchiID,name,withSetsRarityScore,kinship,experience,level,originalOwner
        are copied from the corresponding leaderboard CSV row (originalOwner is normalized).
  - originalWeight       = (1.0 / position) ** exponent_for_that_leaderboard
  - weightAmongEligible  = originalWeight / sum_of_originalWeights_over_all_eligible_entries
  - reward               = leaderboard_pool * weightAmongEligible

Only entries whose originalOwner is in the eligible wallet set are written to these
per-entry CSV files.

Output
======
Writes a CSV file containing all eligible wallets and their combined reward amount.

Output schema:
    wallet,rewardRF

Notes:
- Eligible wallets that have no entries in any leaderboard will receive reward = 0.
- BRSRewards.csv, KINRewards.csv, XPRewards.csv are written into the current working
  directory (one line per eligible gotchi entry, for the respective leaderboard).

Usage
=====
    python CalculateRFRewards.py \
        EligibleWalletsLatest.csv \
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
      VotingData/EligibleWalletsLatest.csv \
      RF/leaderboard_withSetsRarityScore_block_37694538.csv \
      RF/leaderboard_kinship_block_37694538.csv \
      RF/leaderboard_experience_block_37694538.csv \
      296.26 \
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


def load_eligible_wallets(path: str) -> Set[str]:
    """
    Load eligible wallets from EligibleWalletsLatest.csv (schema: wallet,num_proposals,weight).
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


# ---------------------------------------------------------------------------
# Core leaderboard computation
# ---------------------------------------------------------------------------

def compute_owner_rewards_from_ranking(
    ranking_path: str,
    eligible_wallets: Set[str],
    pool_amount: float,
    exponent: float,
) -> Tuple[Dict[str, float], List[Dict[str, str]]]:
    """
    Compute per-owner reward allocation for one ranking list AND per-entry details.

    Steps:
      - Load ranking CSV and filter rows to eligible owners.
      - Compute entry weights: (1/position)^exponent.
      - Sum all entry weights (over eligible entries).
      - Normalize entry weights to sum to 1.0 (weightAmongEligible).
      - Multiply by pool_amount to get entry rewards.
      - Sum entry rewards by owner (per-owner rewards).

    Returns:
      (
        owner_rewards,     # dict owner -> reward_from_this_ranking
        entry_details      # list of dicts for writing per-entry CSV:
                           #   {
                           #       "position",
                           #       "gotchiID",
                           #       "name",
                           #       "withSetsRarityScore",
                           #       "kinship",
                           #       "experience",
                           #       "level",
                           #       "originalOwner",
                           #       "originalWeight",
                           #       "weightAmongEligible",
                           #       "reward"
                           #   }
      )
    """
    # We need more columns than in iter_rank_entries, so read full rows here.
    entry_rows: List[Dict[str, str]] = []
    with open(ranking_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols = set(reader.fieldnames or [])
        required_cols = {
            "position",
            "gotchiID",
            "name",
            "withSetsRarityScore",
            "kinship",
            "experience",
            "level",
            "originalOwner",
        }
        missing = required_cols - cols
        if missing:
            raise ValueError(
                f"{ranking_path} is missing required columns: {', '.join(sorted(missing))}"
            )

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

            # Filter to eligible owners.
            if owner not in eligible_wallets:
                continue

            # Keep row plus parsed position and normalized owner.
            entry_rows.append(
                {
                    "position": str(pos),
                    "gotchiID": (row.get("gotchiID") or "").strip(),
                    "name": (row.get("name") or "").strip(),
                    "withSetsRarityScore": (row.get("withSetsRarityScore") or "").strip(),
                    "kinship": (row.get("kinship") or "").strip(),
                    "experience": (row.get("experience") or "").strip(),
                    "level": (row.get("level") or "").strip(),
                    "originalOwner": owner,
                }
            )

    owner_rewards: Dict[str, float] = {}
    entry_details: List[Dict[str, str]] = []

    if not entry_rows or pool_amount <= 0:
        # No eligible entries or no pool; all rewards remain zero.
        return owner_rewards, entry_details

    # Compute original weights
    for e in entry_rows:
        pos = int(e["position"])
        w = (1.0 / float(pos)) ** exponent
        e["originalWeight"] = w

    total_w = sum(float(e["originalWeight"]) for e in entry_rows)
    if total_w <= 0:
        # Degenerate case: no positive weights.
        return owner_rewards, entry_details

    # Normalize and compute per-entry rewards; aggregate per-owner.
    for e in entry_rows:
        w_orig = float(e["originalWeight"])
        w_norm = w_orig / total_w
        reward = w_norm * pool_amount

        e["weightAmongEligible"] = w_norm
        e["reward"] = reward

        owner = e["originalOwner"]
        owner_rewards[owner] = owner_rewards.get(owner, 0.0) + reward

        # Prepare a stringified copy for CSV output.
        entry_details.append(
            {
                "position": e["position"],
                "gotchiID": e["gotchiID"],
                "name": e["name"],
                "withSetsRarityScore": e["withSetsRarityScore"],
                "kinship": e["kinship"],
                "experience": e["experience"],
                "level": e["level"],
                "originalOwner": owner,
                "originalWeight": f"{w_orig:.12f}",
                "weightAmongEligible": f"{w_norm:.12f}",
                "reward": f"{reward:.12f}",
            }
        )

    return owner_rewards, entry_details


def write_rewards_output(
    output_path: str,
    eligible_wallets: Set[str],
    total_rewards: Dict[str, float],
) -> None:
    """
    Write output CSV:
      wallet,rewardRF

    Includes ALL eligible wallets, even if reward=0.
    Sorted by reward desc then wallet asc for determinism.
    """
    rows: List[Tuple[str, float]] = []
    for w in eligible_wallets:
        rows.append((w, float(total_rewards.get(w, 0.0))))

    rows.sort(key=lambda x: (-x[1], x[0]))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "rewardRF"])
        for w, r in rows:
            writer.writerow([w, f"{r:.10f}"])


def write_leaderboard_rewards_details(
    output_path: str,
    entry_details: List[Dict[str, str]],
) -> None:
    """
    Write per-entry leaderboard rewards CSV with schema:

        position,gotchiID,name,withSetsRarityScore,kinship,experience,level,
        originalOwner,originalWeight,weightAmongEligible,reward

    Only entries with eligible owners (already filtered upstream) are included.
    """
    header = [
        "position",
        "gotchiID",
        "name",
        "withSetsRarityScore",
        "kinship",
        "experience",
        "level",
        "originalOwner",
        "originalWeight",
        "weightAmongEligible",
        "reward",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for e in entry_details:
            writer.writerow(e)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 11:
        print(
            "Usage:\n"
            "  python CalculateRFRewards.py "
            "EligibleWalletsLatest.csv BRSRanking.csv KINRankings.csv XPRankings.csv RewardAmount RewardPercentages OutputRewards.csv BRSRewardDetails.csv KINRewardDetails.csv XPRewardDetails.csv\n\n"
            "RewardPercentages examples: \"50,30,20\" or \"0.5,0.3,0.2\"",
            file=sys.stderr,
        )
        sys.exit(1)

    eligible_wallets_path = sys.argv[1]
    brs_path = sys.argv[2]
    kin_path = sys.argv[3]
    xp_path = sys.argv[4]
    reward_amount_raw = sys.argv[5]
    reward_percentages_raw = sys.argv[6]
    output_path = sys.argv[7]

    # Fixed filenames for per-entry leaderboard rewards.
    brs_rewards_csv_path = sys.argv[8]
    kin_rewards_csv_path = sys.argv[9]
    xp_rewards_csv_path = sys.argv[10]

    eligible_wallets = load_eligible_wallets(eligible_wallets_path)
    if not eligible_wallets:
        print("No eligible wallets found in EligibleWalletsLatest.csv; output will be empty rewards.", file=sys.stderr)

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

    # Compute per-owner rewards and per-entry details for each ranking.
    brs_rewards, brs_entries = compute_owner_rewards_from_ranking(
        ranking_path=brs_path,
        eligible_wallets=eligible_wallets,
        pool_amount=brs_pool,
        exponent=0.94,
    )
    kin_rewards, kin_entries = compute_owner_rewards_from_ranking(
        ranking_path=kin_path,
        eligible_wallets=eligible_wallets,
        pool_amount=kin_pool,
        exponent=0.76,
    )
    xp_rewards, xp_entries = compute_owner_rewards_from_ranking(
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

    # Write per-wallet output
    write_rewards_output(output_path, eligible_wallets, total_rewards)

    # Write per-entry leaderboard CSVs
    write_leaderboard_rewards_details(brs_rewards_csv_path, brs_entries)
    write_leaderboard_rewards_details(kin_rewards_csv_path, kin_entries)
    write_leaderboard_rewards_details(xp_rewards_csv_path, xp_entries)

    # Optional diagnostics to stderr
    total_out = sum(total_rewards.get(w, 0.0) for w in eligible_wallets)
    print(f"Eligible wallets: {len(eligible_wallets)}", file=sys.stderr)
    print(f"RewardAmount: {reward_amount}", file=sys.stderr)
    print(f"Pools -> BRS: {brs_pool}, KIN: {kin_pool}, XP: {xp_pool}", file=sys.stderr)
    print(f"Total rewards assigned (eligible wallets): {total_out}", file=sys.stderr)
    print(f"Wrote per-wallet rewards: {output_path}", file=sys.stderr)
    print(f"Wrote BRS per-entry rewards: {brs_rewards_csv_path}", file=sys.stderr)
    print(f"Wrote KIN per-entry rewards: {kin_rewards_csv_path}", file=sys.stderr)
    print(f"Wrote XP per-entry rewards: {xp_rewards_csv_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
