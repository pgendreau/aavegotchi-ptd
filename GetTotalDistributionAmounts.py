#!/usr/bin/env python3
"""
GetTotalDistributionAmounts.py

Purpose
=======
This script combines three different reward components into a single per-wallet payout table:

  1) RF rewards              (from OutputRFRewards.csv)
  2) VP-based rewards        (derived from CombinedVP.csv)
  3) Voting-weighted VP      (VP normalized, then weighted by voting participation from OutputWeights.csv)

It produces one final CSV (TotalDistributionAmounts.csv) sorted by total rewards descending.

Inputs
======
1) CombinedVP.csv
   Schema:
       wallet,combinedVP
   Meaning:
       - combinedVP is a floating point voting power number per wallet.
       - wallet addresses may have inconsistent casing; we normalize to lowercase.

2) OutputWeights.csv
   Schema:
       wallet,num_proposals,weight
   Meaning:
       - 'weight' is a voting participation percentage expressed as a fraction in [0,1]
         (e.g. 0.75 means voted on 75% of decisions).
       - We use this 'weight' as "votingPercentage" in the output.
       - wallet addresses normalized to lowercase.

3) OutputRFRewards.csv
   Schema:
       wallet,rewardRF
   Meaning:
       - rewardRF is a floating point reward amount per wallet (can be 0).
       - wallet addresses normalized to lowercase.

Additional scalar inputs (CLI)
==============================
4) RewardAmountVP
   - A float: total reward pool allocated proportional to normalized combinedVP.

5) RewardAmountVotingWeightedVP
   - A float: total reward pool allocated proportional to:
         normalized_combinedVP * votingParticipationWeight
     followed by renormalization so weights sum to 1.

Computation Details
===================
Let:
  vp_i = combinedVP for wallet i
  vp_total = sum_i vp_i

Step A — Normalized VP weights
  vp_weight_i = vp_i / vp_total      (if vp_total > 0)

Step B — RewardVP distribution
  rewardVP_i = RewardAmountVP * vp_weight_i

Step C — Voting-weighted VP weights (then renormalized)
  raw_weighted_i = vp_weight_i * voting_participation_i
  raw_sum = sum_i raw_weighted_i

  voting_weighted_weight_normalized_i =
      raw_weighted_i / raw_sum       (if raw_sum > 0)

Step D — RewardWeightedVP distribution
  rewardWeightedVP_i = RewardAmountVotingWeightedVP * voting_weighted_weight_normalized_i

Step E — Total
  rewardTotal_i = rewardRF_i + rewardVP_i + rewardWeightedVP_i

Important matching behavior
===========================
- Wallet addresses are normalized to lowercase (strip + lowercase).
- The output includes wallets appearing in ANY input file (union of keys),
  but uses 0.0 for missing values:
    * missing combinedVP -> 0
    * missing voting weight -> 0
    * missing rewardRF -> 0
- If vp_total == 0, then all RewardVP values are 0.
- If raw_sum == 0, then all RewardWeightedVP values are 0.

Output
======
Writes TotalDistributionAmounts.csv with rows sorted by rewardTotal desc, then wallet asc.

Schema:
    wallet,
    combinedVP,
    votingPercentage,
    rewardVP,
    rewardWeightedVP,
    rewardRF,
    rewardTotal

Usage
=====
    python GetTotalDistributionAmounts.py \
        CombinedVP.csv OutputWeights.csv OutputRFRewards.csv \
        RewardAmountVP RewardAmountVotingWeightedVP \
        TotalDistributionAmounts.csv

Example:
    python GetTotalDistributionAmounts.py CombinedVP.csv OutputWeights.csv OutputRFRewards.csv 25000 25000 TotalDistributionAmounts.csv
"""

import csv
import sys
from typing import Dict, Set, Tuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_addr(addr: str) -> str:
    """Normalize wallet address by stripping whitespace and lowercasing."""
    if not addr:
        return ""
    return addr.strip().lower()


def load_combined_vp(path: str) -> Dict[str, float]:
    """Load CombinedVP.csv (wallet,combinedVP) into dict wallet->combinedVP."""
    out: Dict[str, float] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "wallet" not in (reader.fieldnames or []) or "combinedVP" not in (reader.fieldnames or []):
            raise ValueError(f"{path} must have columns: wallet,combinedVP")

        for row in reader:
            w = normalize_addr(row.get("wallet") or "")
            if not w:
                continue
            try:
                vp = float(row.get("combinedVP") or 0.0)
            except Exception:
                vp = 0.0
            out[w] = vp
    return out


def load_voting_weights(path: str) -> Dict[str, float]:
    """Load OutputWeights.csv (wallet,num_proposals,weight) into dict wallet->weight."""
    out: Dict[str, float] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "wallet" not in (reader.fieldnames or []) or "weight" not in (reader.fieldnames or []):
            raise ValueError(f"{path} must have columns including: wallet,weight")

        for row in reader:
            w = normalize_addr(row.get("wallet") or "")
            if not w:
                continue
            try:
                wt = float(row.get("weight") or 0.0)
            except Exception:
                wt = 0.0
            out[w] = wt
    return out


def load_rf_rewards(path: str) -> Dict[str, float]:
    """Load OutputRFRewards.csv (wallet,rewardRF) into dict wallet->rewardRF."""
    out: Dict[str, float] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "wallet" not in (reader.fieldnames or []) or "rewardRF" not in (reader.fieldnames or []):
            raise ValueError(f"{path} must have columns: wallet,rewardRF")

        for row in reader:
            w = normalize_addr(row.get("wallet") or "")
            if not w:
                continue
            try:
                r = float(row.get("rewardRF") or 0.0)
            except Exception:
                r = 0.0
            out[w] = r
    return out


def union_wallets(*dicts: Dict[str, float]) -> Set[str]:
    """Return the union of keys from multiple dicts."""
    s: Set[str] = set()
    for d in dicts:
        s.update(d.keys())
    return s


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 7:
        print(
            "Usage:\n"
            "  python GetTotalDistributionAmounts.py "
            "CombinedVP.csv OutputWeights.csv OutputRFRewards.csv "
            "RewardAmountVP RewardAmountVotingWeightedVP TotalDistributionAmounts.csv",
            file=sys.stderr,
        )
        sys.exit(1)

    combined_vp_path = sys.argv[1]
    outputweights_path = sys.argv[2]
    rf_rewards_path = sys.argv[3]
    reward_amount_vp_raw = sys.argv[4]
    reward_amount_weighted_vp_raw = sys.argv[5]
    out_path = sys.argv[6]

    try:
        reward_amount_vp = float(reward_amount_vp_raw)
        reward_amount_weighted_vp = float(reward_amount_weighted_vp_raw)
    except Exception:
        raise ValueError("RewardAmountVP and RewardAmountVotingWeightedVP must be floats.")

    if reward_amount_vp < 0 or reward_amount_weighted_vp < 0:
        raise ValueError("Reward amounts must be >= 0.")

    # Load inputs
    combined_vp = load_combined_vp(combined_vp_path)
    voting_weight = load_voting_weights(outputweights_path)
    reward_rf = load_rf_rewards(rf_rewards_path)

    wallets = union_wallets({k: 0.0 for k in combined_vp.keys()},
                            {k: 0.0 for k in voting_weight.keys()},
                            {k: 0.0 for k in reward_rf.keys()})

    # Step 1: normalize combinedVP
    total_vp = sum(max(0.0, v) for v in combined_vp.values())
    vp_weight: Dict[str, float] = {}
    if total_vp > 0:
        for w in wallets:
            vp = max(0.0, combined_vp.get(w, 0.0))
            vp_weight[w] = vp / total_vp
    else:
        for w in wallets:
            vp_weight[w] = 0.0

    # Step 2: rewardVP
    reward_vp: Dict[str, float] = {w: reward_amount_vp * vp_weight[w] for w in wallets}

    # Step 3: voting-weighted VP + renormalize
    raw_weighted: Dict[str, float] = {}
    for w in wallets:
        raw_weighted[w] = vp_weight[w] * max(0.0, voting_weight.get(w, 0.0))

    raw_sum = sum(raw_weighted.values())
    voting_weighted_norm: Dict[str, float] = {}
    if raw_sum > 0:
        for w in wallets:
            voting_weighted_norm[w] = raw_weighted[w] / raw_sum
    else:
        for w in wallets:
            voting_weighted_norm[w] = 0.0

    # Step 4: rewardWeightedVP
    reward_weighted_vp: Dict[str, float] = {w: reward_amount_weighted_vp * voting_weighted_norm[w] for w in wallets}

    # Step 5: rewardTotal
    reward_total: Dict[str, float] = {}
    for w in wallets:
        rf = reward_rf.get(w, 0.0)
        reward_total[w] = rf + reward_vp[w] + reward_weighted_vp[w]

    # Step 6: write output sorted by rewardTotal desc
    rows = []
    for w in wallets:
        rows.append((
            w,
            combined_vp.get(w, 0.0),
            voting_weight.get(w, 0.0),   # votingPercentage (original unaltered "weight")
            reward_vp[w],
            reward_weighted_vp[w],
            reward_rf.get(w, 0.0),
            reward_total[w],
        ))

    rows.sort(key=lambda x: (-x[6], x[0]))

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "wallet",
            "combinedVP",
            "votingPercentage",
            "rewardVP",
            "rewardWeightedVP",
            "rewardRF",
            "rewardTotal",
        ])
        for (w, vp, vpct, rvp, rweighted, rrf, rtot) in rows:
            writer.writerow([
                w,
                f"{vp:.10f}",
                f"{vpct:.10f}",
                f"{rvp:.10f}",
                f"{rweighted:.10f}",
                f"{rrf:.10f}",
                f"{rtot:.10f}",
            ])

    # Diagnostics to stderr
    print(f"Wallets in output: {len(rows)}", file=sys.stderr)
    print(f"Total combinedVP: {total_vp}", file=sys.stderr)
    print(f"RewardAmountVP: {reward_amount_vp}", file=sys.stderr)
    print(f"RewardAmountVotingWeightedVP: {reward_amount_weighted_vp}", file=sys.stderr)
    print(f"Sum rewardVP: {sum(reward_vp.values()):.10f}", file=sys.stderr)
    print(f"Sum rewardWeightedVP: {sum(reward_weighted_vp.values()):.10f}", file=sys.stderr)
    print(f"Sum rewardRF: {sum(reward_rf.get(w, 0.0) for w in wallets):.10f}", file=sys.stderr)
    print(f"Sum rewardTotal: {sum(reward_total.values()):.10f}", file=sys.stderr)
    print(f"Wrote: {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
