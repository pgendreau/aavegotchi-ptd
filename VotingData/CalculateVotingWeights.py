#!/usr/bin/env python3
"""
SECOND STAGE (ACTIVE PROPOSALS) — Update cached vote counts with data from new (e.g. still active) proposals.

This script is intended to be the *second* script in a 2-step pipeline:

STEP 1 (CalculateCachedVoteCounts):
  - Computes vote counts for concluded proposals only (that data no longer changes).
  - Outputs:
      1) VoteCounts.csv                 -> vote counts for ALL wallets (eligible or not)
      2) EligibleWallets.csv            -> wallets that are eligible based on AGIP6M rule (per-wallet eligibility)
      3) ConcludedDecisionCount.txt     -> (# of concluded "countable decisions" used when producing VoteCounts.csv)

STEP 2 (this script):
  - Inputs:
      - VoteCounts.csv              (ALL wallets, counts from concluded proposals)
      - EligibleWallets.csv         (eligible wallets after concluded proposals)
      - ActiveProposals.csv         (proposals that should amend counts; may include signal proposals)
      - AGIPActive.csv             (subset of ActiveProposals; ONLY proposals that can make a wallet eligible)
      - WalletAliases.csv           (master + slave wallets)
      - ConcludedDecisionCount.txt  (# of concluded "countable decisions" used when producing VoteCounts.csv)

  - The script fetches votes ONLY for proposals in ActiveProposals.csv and amends counts.

  - Eligibility update:
        A wallet is eligible if *that wallet itself* voted on ANY proposal in AGIPActive.csv.
        (No aliasing for eligibility. A slave does NOT make master eligible, and vice versa.)

  - Counting update:
        For each proposal in ActiveProposals.csv, each voter is mapped through wallet aliases:
            identity = alias_map.get(voter, voter)
        and each identity gets at most +1 for that proposal (even if both master and slave voted).

  - Output:
      - OutputWeights.csv (only eligible wallets, after applying AGIPActive updates)

Important notes / assumptions
=============================
- There are NO mirrors in step 2 (no GV2AV in this script).
- Each proposal ID in ActiveProposals.csv counts as one decision for vote-count purposes.
- VoteCounts.csv counts previously concluded proposals; this script only adds decisions from new ActiveProposals.csv.
- Total decisions for weight = (#concluded_decisions_from_cache + #active_proposals_counted).

File Formats
============
VoteCounts.csv:
    wallet,num_proposals
  - wallet should be lowercase (this script normalizes anyway)
  - includes ALL wallets/identities seen in concluded proposals
  - num_proposals is int

EligibleWallets.csv:
    wallet
  - one wallet address per row (header optional, tolerated)
  - wallets eligible after concluded proposals

ConcludedDecisionCount.txt:
  - a single integer N, the number of concluded proposals counted in VoteCounts.csv

ActiveProposals.csv:
    id,title,author,date_utc,num_wallets
  - we only use id

AGIPActive.csv:
    id,title,author,date_utc,num_wallets
  - we only use id
  - voting on ANY of these proposals makes the *wallet itself* eligible

WalletAliases.csv:
  - Each row: master,slave1,slave2,slave3 (2 to 4 addresses total)

OutputWeights.csv:
    wallet,num_proposals,weight
  - ONLY eligible wallets (after updating with AGIPActive votes)
  - Eligible slave wallets are included but forced to 0,0.0 (no double-dipping)

Usage
=====
    python CalculateVotingWeights.py \
        VoteCounts.csv EligibleWallets.csv ConcludedDecisionCount.txt ActiveProposals.csv \
        AGIPActive.csv WalletAliases.csv OutputWeights.csv
"""

import csv
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Set, Tuple

import requests

SNAPSHOT_GRAPHQL = "https://hub.snapshot.org/graphql"

VOTES_PAGE_SIZE = 1000
RETRY_MAX = 5
RETRY_BACKOFF = 1.6
TIMEOUT = 30


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_addr(addr: str) -> str:
    """Strip whitespace and lowercase. Always use this for wallet addresses."""
    if not addr:
        return ""
    return addr.strip().lower()


def read_int_file(path: str) -> int:
    """Read a file containing a single integer."""
    with open(path, "r", encoding="utf-8") as f:
        s = f.read().strip()
    if not s:
        raise ValueError(f"{path} is empty; expected an integer.")
    return int(s)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Proposal:
    id: str


# ---------------------------------------------------------------------------
# CSV loading utilities
# ---------------------------------------------------------------------------

def load_proposals_csv_ids(path: str) -> List[Proposal]:
    """
    Load a proposal CSV with a header containing at least: id
    (e.g. ActiveProposals.csv or AGIPActive.csv). Only 'id' is used.
    """
    proposals: List[Proposal] = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "id" not in (reader.fieldnames or []):
            raise ValueError(f"{path} must have an 'id' column in the header.")
        for row in reader:
            pid = (row.get("id") or "").strip()
            if pid:
                proposals.append(Proposal(id=pid))
    return proposals


def load_vote_counts(path: str) -> Dict[str, int]:
    """
    Load VoteCounts.csv into a dict: wallet -> num_proposals (int).

    Expected header:
      wallet,num_proposals

    Any wallet is normalized to lowercase.
    """
    counts: Dict[str, int] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"{path} has no header.")
        if "wallet" not in reader.fieldnames or "num_proposals" not in reader.fieldnames:
            raise ValueError(f"{path} must have columns wallet,num_proposals")

        for row in reader:
            w = normalize_addr(row.get("wallet") or "")
            if not w:
                continue
            try:
                n = int(float(row.get("num_proposals") or 0))
            except Exception:
                n = 0
            counts[w] = n
    return counts


def load_wallet_list_csv(path: str) -> Set[str]:
    """
    Load a CSV containing a single wallet per row.

    Accepts either:
      - header 'wallet' and subsequent rows in that column, or
      - no header: one wallet per row.

    Any wallet is normalized to lowercase.
    """
    wallets: Set[str] = set()

    with open(path, "r", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))

    if not rows:
        return wallets

    first = [c.strip() for c in rows[0]]
    if len(first) >= 1 and first[0].strip().lower() == "wallet":
        for r in rows[1:]:
            if not r:
                continue
            w = normalize_addr(r[0])
            if w:
                wallets.add(w)
    else:
        for r in rows:
            if not r:
                continue
            w = normalize_addr(r[0])
            if w:
                wallets.add(w)

    return wallets


def load_alias_map(path: str) -> Tuple[Dict[str, str], Set[str]]:
    """
    Load WalletAliases.csv and return:
      - alias_map: normalized wallet -> normalized master
      - slaves: set of normalized slave wallets

    Allows optional header row.
    """
    alias_map: Dict[str, str] = {}
    slaves: Set[str] = set()

    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        first_row = True
        for row in reader:
            if not row:
                continue
            raw_cells = [c.strip() for c in row if c.strip()]
            if not raw_cells:
                continue

            # Skip header if first cell isn't an address
            if first_row and not raw_cells[0].lower().startswith("0x"):
                first_row = False
                continue
            first_row = False

            if len(raw_cells) < 2:
                continue

            cells = [normalize_addr(c) for c in raw_cells]
            master = cells[0]
            alias_map.setdefault(master, master)

            for s in cells[1:]:
                if not s or s == master:
                    continue
                alias_map[s] = master
                slaves.add(s)

    return alias_map, slaves


# ---------------------------------------------------------------------------
# Snapshot GraphQL (rate-limited)
# ---------------------------------------------------------------------------

def gql(session: requests.Session, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    Rate-limited GraphQL POST with retries.
    Sleeps >= 1s per attempt (and extra backoff on retry).
    """
    payload = {"query": query, "variables": variables}
    last_exc = None

    for attempt in range(1, RETRY_MAX + 1):
        time.sleep(1.0)
        try:
            resp = session.post(SNAPSHOT_GRAPHQL, json=payload, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data and data["errors"]:
                raise RuntimeError(f"GraphQL error: {data['errors']}")
            return data["data"]
        except Exception as e:
            last_exc = e
            if attempt == RETRY_MAX:
                break
            time.sleep(RETRY_BACKOFF ** (attempt - 1))

    raise RuntimeError(f"Request failed after {RETRY_MAX} attempts: {last_exc}")


def iter_votes_for_proposal(session: requests.Session, proposal_id: str) -> Iterable[str]:
    """Yield normalized voter addresses for all votes on a proposal (pagination)."""
    query = """
    query ($proposal: String!, $first: Int!, $skip: Int!) {
      votes(
        where: { proposal: $proposal }
        first: $first
        skip: $skip
        orderBy: "created"
        orderDirection: asc
      ) {
        voter
      }
    }
    """

    skip = 0
    while True:
        data = gql(session, query, {"proposal": proposal_id, "first": VOTES_PAGE_SIZE, "skip": skip})
        votes = data.get("votes", [])
        if not votes:
            break

        for v in votes:
            voter = v.get("voter")
            if voter:
                yield normalize_addr(voter)

        if len(votes) < VOTES_PAGE_SIZE:
            break
        skip += VOTES_PAGE_SIZE


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def update_counts_with_active_proposals(
    session: requests.Session,
    active_proposals: List[Proposal],
    counts_all_wallets: Dict[str, int],
    alias_map: Dict[str, str],
) -> Dict[str, int]:
    """
    For each proposal in ActiveProposals.csv:
      - Fetch voters (raw wallets).
      - For counting: map each voter to identity via alias_map and add +1 per identity per proposal.

    Returns:
      updated counts_all_wallets (identity counts).
    """
    total = len(active_proposals)
    for i, p in enumerate(active_proposals, start=1):
        print(f"Processing ACTIVE proposal (counting) {i}/{total}: {p.id}", file=sys.stderr)

        voters_raw: Set[str] = set(iter_votes_for_proposal(session, p.id))

        # Merge slaves->master for counting. Ensure at most +1 per identity per proposal.
        identities: Set[str] = set()
        for w in voters_raw:
            identity = alias_map.get(w, w)
            identities.add(identity)

        for identity in identities:
            counts_all_wallets[identity] = counts_all_wallets.get(identity, 0) + 1

    return counts_all_wallets


def compute_newly_eligible_from_AGIPActive(
    session: requests.Session,
    AGIPActive_proposals: List[Proposal],
) -> Set[str]:
    """
    Eligibility update:

    A wallet becomes eligible if *that wallet itself* voted on ANY proposal in AGIPActive.csv.
    - No mirroring (none in this stage).
    - No aliasing: slave votes make the slave eligible, not the master.

    Returns:
      Set of newly eligible wallets from AGIPActive votes (raw wallets).
    """
    newly_eligible: Set[str] = set()

    total = len(AGIPActive_proposals)
    for i, p in enumerate(AGIPActive_proposals, start=1):
        print(f"Processing AGIPActive (eligibility) {i}/{total}: {p.id}", file=sys.stderr)
        for voter in iter_votes_for_proposal(session, p.id):
            newly_eligible.add(voter)

    return newly_eligible


def write_output_weights(
    output_path: str,
    eligible_wallets: Set[str],
    counts_all_wallets: Dict[str, int],
    slaves: Set[str],
    total_decisions: int,
) -> None:
    """
    Write OutputWeights.csv with:
        wallet,num_proposals,weight
    Only wallets in eligible_wallets are written.

    Rule:
      - If wallet is a slave -> force (0, 0.0)
      - Else -> use counts_all_wallets.get(wallet,0) and compute weight against total_decisions
    """
    rows: List[Tuple[str, int, float]] = []

    for wallet in eligible_wallets:
        if wallet in slaves:
            n = 0
            w = 0.0
        else:
            n = counts_all_wallets.get(wallet, 0)
            w = (n / total_decisions) if total_decisions > 0 else 0.0
        rows.append((wallet, n, w))

    rows.sort(key=lambda x: (-x[2], x[0]))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "num_proposals", "weight"])
        for wallet, n, w in rows:
            writer.writerow([wallet, n, f"{w:.6f}"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 8:
        print(
            "Usage:\n"
            "  python CalculateVotingWeights.py VoteCounts.csv EligibleWallets.csv ActiveProposals.csv AGIPActive.csv "
            "WalletAliases.csv ConcludedDecisionCount.txt OutputWeights.csv",
            file=sys.stderr,
        )
        sys.exit(1)

    vote_counts_path = sys.argv[1]
    eligible_wallets_path = sys.argv[2]
    concluded_decisions_path = sys.argv[3]
    active_proposals_path = sys.argv[4]
    AGIPActive_path = sys.argv[5]
    aliases_path = sys.argv[6]
    output_path = sys.argv[7]

    # Load inputs
    counts_all = load_vote_counts(vote_counts_path)
    eligible_wallets = load_wallet_list_csv(eligible_wallets_path)

    active_proposals = load_proposals_csv_ids(active_proposals_path)
    AGIPActive_proposals = load_proposals_csv_ids(AGIPActive_path)

    alias_map, slaves = load_alias_map(aliases_path)
    concluded_decisions = read_int_file(concluded_decisions_path)

    # Total decisions used for weight = concluded + active-counted
    total_decisions = concluded_decisions + len(active_proposals)

    print(f"Loaded concluded vote counts for {len(counts_all)} wallets/identities.", file=sys.stderr)
    print(f"Loaded eligible wallets (pre-active): {len(eligible_wallets)}", file=sys.stderr)
    print(f"Loaded active proposals (counting): {len(active_proposals)}", file=sys.stderr)
    print(f"Loaded AGIPActive proposals (eligibility): {len(AGIPActive_proposals)}", file=sys.stderr)
    print(f"Loaded slave wallets from aliases: {len(slaves)}", file=sys.stderr)
    print(f"Concluded decisions: {concluded_decisions}", file=sys.stderr)
    print(f"TOTAL decisions for weights (concluded + active): {total_decisions}", file=sys.stderr)

    with requests.Session() as session:
        session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "snapshot-active-amend/1.1",
            }
        )

        # 1) Update counts using ALL active proposals (counting set).
        counts_all = update_counts_with_active_proposals(
            session=session,
            active_proposals=active_proposals,
            counts_all_wallets=counts_all,
            alias_map=alias_map,
        )

        # 2) Update eligibility using ONLY AGIPActive proposals (eligibility set).
        newly_eligible_from_agip = compute_newly_eligible_from_AGIPActive(
            session=session,
            AGIPActive_proposals=AGIPActive_proposals,
        )

    # Update eligible set (per-wallet, no aliasing).
    eligible_wallets_updated = set(eligible_wallets)
    eligible_wallets_updated.update(newly_eligible_from_agip)

    print(f"Newly eligible wallets from AGIPActive: {len(newly_eligible_from_agip)}", file=sys.stderr)
    print(f"Eligible wallets (post-active): {len(eligible_wallets_updated)}", file=sys.stderr)

    # Write final OutputWeights.csv for eligible wallets only.
    write_output_weights(
        output_path=output_path,
        eligible_wallets=eligible_wallets_updated,
        counts_all_wallets=counts_all,
        slaves=slaves,
        total_decisions=total_decisions,
    )

    print(f"Wrote: {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
