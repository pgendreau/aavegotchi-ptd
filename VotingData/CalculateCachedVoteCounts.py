#!/usr/bin/env python3
"""
STEP 1 (CalculateCachedVoteCounts) — Create a cache for concluded proposals.
This is the FIRST script in a 2-step pipeline.

Compute per-wallet participation (vote counts) for Aavegotchi Snapshot proposals, with:
- Mirrored proposals on GotchiVault (sub-dao)
- Wallet aliasing (master + previous "slave" wallets merged for counting)
- Lowercased, normalized wallet addresses throughout

Inputs
======
1) IncludedProposals.csv
   Columns: id,title,author,date_utc,num_wallets
   Meaning:
       - This file contains ONLY the Aavegotchi main-dao proposals (av_ids).
       - These proposals are assumed to be *concluded* for this cache step.
       - Some of these main-dao proposals were mirrored as separate proposals
         on a sub-dao (GotchiVault).
       - These mirrored pairs are listed in GV2AV.csv (see below).

2) AGIP6M.csv
   Columns: id,title,author,date_utc,num_wallets
   Meaning:
       - This is a subset of IncludedProposals.csv (main-dao only).
       - It is used *only* to determine **per-wallet eligibility**:
           A wallet (address) is eligible if *that wallet itself* has voted
           on at least one proposal in this file.
       - IMPORTANT:
           * These are main-dao proposals only.
           * We do NOT use GV2AV mirroring for this eligibility step.
           * We also do NOT apply aliasing for eligibility:
               - A slave wallet does NOT make its master eligible.
               - A master wallet does NOT make its slaves eligible.
           * Eligibility is purely: "did THIS wallet vote on an AGIP6M proposal?"

3) GV2AV.csv
   Columns: gv_id,av_id
   Meaning:
       - Each row links a GotchiVault proposal (gv_id) to its mirrored Aavegotchi
         proposal (av_id).
       - For participation counting, gv_id and av_id represent the SAME
         underlying governance decision:
           * If a wallet voted only on gv_id, it counts as if they voted on
             that main-dao decision.
           * If a wallet voted only on av_id, same.
           * If a wallet voted on BOTH gv_id and av_id, that still only counts
             as +1 decision, not +2.

4) WalletAliases.csv
   Meaning:
       - Each row contains between 2 and 4 wallet addresses.
       - The first wallet in the row is the **master** wallet (newest wallet).
       - The following wallets (if any) are older wallets by the same owner
         ("slave" wallets).
       - Example row:
           0xMASTER,0xOLD1,0xOLD2
         This means:
           * 0xMASTER is the canonical identity for this person.
           * Votes cast by 0xOLD1 and 0xOLD2 should be treated as if they were
             made by 0xMASTER for the purposes of participation counting.
           * If both 0xMASTER and 0xOLD1 voted on the same decision, it still
             only counts as +1 decision for that master identity.

Outputs
=======
1) VoteCounts.csv
   - Vote counts for ALL wallets (eligible or not).
   - Counting is performed on "identities" using aliasing:
       * votes by slave wallets are credited to the master identity
       * each decision adds at most +1 to an identity (even if a master+slave both voted)
   - Additionally, all slave wallets from WalletAliases.csv are written explicitly with:
       * num_proposals = 0
     (This ensures slaves never carry counts in the cache, since masters already received credit.)

   CSV columns:
       wallet,num_proposals
   Where "wallet" is either:
       - an identity address (master or a non-aliased wallet) with its decision count, or
       - a slave wallet address included explicitly with num_proposals = 0
   All wallets are normalized to lowercase.

2) EligibleWalletsCached.csv
   - Wallets that are eligible based on AGIP6M rule (per-wallet eligibility).
   - IMPORTANT: eligibility is per-wallet (raw address), WITHOUT aliasing:
       * a slave wallet voting makes the slave eligible, not the master
       * a master wallet voting does not make its slaves eligible

   CSV columns:
       wallet
   One eligible wallet per row (normalized to lowercase).

3) ConcludedDecisionCount.txt
   - A single integer:
       number of concluded "countable decisions" used to produce VoteCounts.csv.
   - In this step, "countable decisions" = number of rows in IncludedProposals.csv
     (i.e. the number of main-dao proposals you fed into this script, assumed concluded).
   - Mirrored proposals do NOT increase this number; each av_id counts as 1 decision.

Behavior summary
================
- **Address normalization**
    All wallet addresses are normalized to lowercase via `normalize_addr()`.
    This applies to:
        - votes returned by Snapshot (voter field)
        - wallet addresses from WalletAliases.csv
        - output CSV wallet columns

- **Decision counting (participation)**
    1. Load IncludedProposals.csv (main-dao av_ids).
    2. For each av_id, build a "decision group":
         - always includes the av_id itself
         - also includes any gv_id mirrors from GV2AV.csv
    3. For each decision group:
         - fetch votes for all proposals in the group
         - map each voter to an identity using WalletAliases (slave -> master; else itself)
         - each unique identity gets +1 for that decision

- **Eligibility**
    4. Load AGIP6M.csv and determine eligible wallets:
         - fetch votes for each AGIP6M proposal
         - add each raw voter wallet directly to the eligible set
         - no mirroring and no aliasing are applied here

- **Writing outputs**
    5. Write VoteCounts.csv for all identities (plus explicit 0-rows for all slaves).
    6. Write EligibleWalletsCached.csv (eligible wallets only).
    7. Write ConcludedDecisionCount.txt (number of IncludedProposals rows).

Rate limiting
=============
This script sleeps at least 1s before each Snapshot GraphQL request attempt (plus backoff on retries).

Usage
=====
    python CalculateCachedVoteCounts.py \
        IncludedProposals.csv AGIP6M.csv GV2AV.csv WalletAliases.csv \
        VoteCounts.csv EligibleWalletsCached.csv ConcludedDecisionCount.txt
"""

import csv
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Set, Tuple

import requests

# Snapshot GraphQL endpoint
SNAPSHOT_GRAPHQL = "https://hub.snapshot.org/graphql"

# Pagination + network settings
VOTES_PAGE_SIZE = 1000
RETRY_MAX = 5
RETRY_BACKOFF = 1.6  # exponential backoff factor for retries
TIMEOUT = 30  # seconds


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_addr(addr: str) -> str:
    """
    Normalize an Ethereum-like address to a canonical form:
        - strip surrounding whitespace
        - lowercase

    This must be used consistently for:
        - addresses from votes (Snapshot API)
        - addresses from WalletAliases.csv
        - output wallet column
    """
    if not addr:
        return ""
    return addr.strip().lower()


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Proposal:
    class Proposal:
        """
        Simple container for a proposal from the CSV.

        Attributes
        ----------
        id : str
            Snapshot proposal ID (0x...).
        """
    id: str


# ---------------------------------------------------------------------------
# Loading CSV data
# ---------------------------------------------------------------------------

def load_proposals_from_csv(path: str) -> List[Proposal]:
    """
    Load proposals from a CSV with columns at least:
        id,title,author,date_utc,num_wallets

    Only the 'id' field is used in this script.

    Parameters
    ----------
    path : str
        Path to IncludedProposals.csv or AGIP6M.csv.

    Returns
    -------
    List[Proposal]
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


def load_av_to_gvs(path: str) -> Dict[str, List[str]]:
    """
    Load GV2AV.csv and build a mapping from main-dao proposal (av_id)
    to a list of its mirrored sub-dao proposals (gv_ids).

    GV2AV.csv columns:
        gv_id,av_id

    For each row:
        - gv_id is the sub-dao proposal ID.
        - av_id is the corresponding main-dao proposal ID (this *will* appear
          in IncludedProposals.csv for the decisions we care about).

    Returns
    -------
    Dict[str, List[str]]
        Map: av_id -> [gv_id1, gv_id2, ...]
        (Usually 0 or 1 gv_ids per av_id, but we support multiple.)
    """
    av_to_gvs: Dict[str, List[str]] = {}
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"gv_id", "av_id"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"{path} must have 'gv_id' and 'av_id' columns in the header.")

        for row in reader:
            gv_id = (row.get("gv_id") or "").strip()
            av_id = (row.get("av_id") or "").strip()
            if not gv_id or not av_id:
                continue
            av_to_gvs.setdefault(av_id, []).append(gv_id)

    return av_to_gvs


def build_decision_groups(
    included_proposals: List[Proposal],
    av_to_gvs: Dict[str, List[str]],
) -> Dict[str, List[Proposal]]:
    """
    Build groups of proposals that belong to the same *main-dao decision*.

    Because IncludedProposals.csv contains only main-dao proposals (av_ids),
    each canonical decision is identified by its av_id. However, some of these
    main-dao proposals were mirrored on the sub-dao (gv_ids) and are stored in
    GV2AV.csv.

    For each main-dao proposal av_id from IncludedProposals:
        - The decision group always includes the main-dao proposal itself.
        - If GV2AV.csv lists any gv_id for that av_id, they are added to the
          same decision group.

    Example:
        IncludedProposals: av_ids ["0xAV1", "0xAV2"]
        GV2AV: (gv_id="0xGVX", av_id="0xAV1")

        decision_groups:
            "0xAV1" -> [Proposal("0xAV1"), Proposal("0xGVX")]
            "0xAV2" -> [Proposal("0xAV2")]

    Parameters
    ----------
    included_proposals : List[Proposal]
        Proposals from IncludedProposals.csv (main-dao only).
    av_to_gvs : Dict[str, List[str]]
        Mapping from av_id -> list of mirrored gv_ids.

    Returns
    -------
    Dict[str, List[Proposal]]
        Mapping:
            decision_id (av_id) -> list of Proposal objects (av_id + gv_ids)
    """
    groups: Dict[str, List[Proposal]] = {}
    for p in included_proposals:
        av_id = p.id
        group: List[Proposal] = [p]
        for gv_id in av_to_gvs.get(av_id, []):
            group.append(Proposal(id=gv_id))
        groups[av_id] = group
    return groups


def load_alias_map(path: str) -> Tuple[Dict[str, str], Set[str], Set[str]]:
    """
    Load wallet aliases from WalletAliases.csv.

    Each row contains between 2 and 4 wallet addresses:
        master_wallet, old_wallet_1, old_wallet_2, ...

    The first wallet is the "master" (newest) wallet;
    the following wallets are "slave" (older) wallets for the same owner.

    The function returns:
        alias_map: dict mapping normalized address -> normalized master_wallet
                   (includes master_wallet -> master_wallet)
        masters  : set of normalized master wallets
        slaves   : set of normalized slave wallets

    Notes:
        - All addresses are normalized via normalize_addr().
        - We allow (but do not require) a header row. If the first cell of a row
          does NOT start with '0x' or '0X', we treat that row as a header.
    """
    alias_map: Dict[str, str] = {}
    masters: Set[str] = set()
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
            masters.add(master)
            alias_map.setdefault(master, master)

            for s in cells[1:]:
                if not s or s == master:
                    continue
                alias_map[s] = master
                slaves.add(s)

    return alias_map, masters, slaves


# ---------------------------------------------------------------------------
# Snapshot GraphQL utility
# ---------------------------------------------------------------------------

def gql(session: requests.Session, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST a GraphQL query to Snapshot's hub with simple retry logic.

    Behavior
    --------
    - Waits at least 1 second before each network attempt to avoid hammering
      the API.
    - Retries up to RETRY_MAX times, with exponential backoff between attempts
      in case of errors.

    Parameters
    ----------
    session : requests.Session
        HTTP session object (reused across calls).
    query : str
        GraphQL query string.
    variables : Dict[str, Any]
        Variables for the GraphQL query.

    Returns
    -------
    Dict[str, Any]
        The "data" field from the JSON response.

    Raises
    ------
    RuntimeError
        If all retry attempts fail or a GraphQL error is returned.
    """
    payload = {"query": query, "variables": variables}
    last_exc = None

    for attempt in range(1, RETRY_MAX + 1):
        time.sleep(1.0)  # minimum delay between attempts

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
    """
    Yield *normalized* voter addresses for all votes on a given proposal,
    handling pagination.

    We only fetch and return the 'voter' field, normalized via normalize_addr().
    The consumer is responsible for counting / deduplicating as needed.

    Parameters
    ----------
    session : requests.Session
        HTTP session object.
    proposal_id : str
        Snapshot proposal ID.

    Yields
    ------
    str
        Normalized wallet address of a voter.
    """
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
# Core computations
# ---------------------------------------------------------------------------

def build_decision_counts(
    session: requests.Session,
    decision_groups: Dict[str, List[Proposal]],
    alias_map: Dict[str, str],
) -> Dict[str, int]:
    """
    Count how many *decisions* each identity (wallet or master wallet) has
    participated in.

    For each decision group (identified by its main-dao av_id):
      - Fetch votes for every proposal in the group (av_id and any gv_ids).
      - For each raw voter address (already normalized by iter_votes_for_proposal):
            identity = alias_map.get(voter, voter)
        (master if voter is a slave, otherwise voter itself).
      - Collect a set of unique identities that voted on ANY of those proposals.
      - Each such identity gets +1 decision_count for that decision.

    This ensures:
      - Voting only on the sub-dao mirror (gv_id) still counts.
      - Voting with old wallets still counts for the master wallet.
      - Voting with both master and slave wallets on the same decision still
        counts as +1, not +2.

    Parameters
    ----------
    session : requests.Session
        HTTP session object.
    decision_groups : Dict[str, List[Proposal]]
        Mapping from main-dao decision id (av_id) to a list of Proposal objects
        (including its mirrors).
    alias_map : Dict[str, str]
        Mapping from normalized wallet -> normalized master wallet (identity).
        Wallets not in this map are identities themselves.

    Returns
    -------
    Dict[str, int]
        identity -> decision_count
    """
    wallet_counts: Dict[str, int] = {}

    total = len(decision_groups)
    for idx, (decision_id, proposals) in enumerate(decision_groups.items(), start=1):
        print(
            f"Counting decision {idx}/{total}: decision_id={decision_id}, proposals_in_group={len(proposals)}",
            file=sys.stderr,
        )

        identities_voted: Set[str] = set()

        for prop in proposals:
            for voter in iter_votes_for_proposal(session, prop.id):
                identity = alias_map.get(voter, voter)
                identities_voted.add(identity)

        for identity in identities_voted:
            wallet_counts[identity] = wallet_counts.get(identity, 0) + 1

    return wallet_counts


def compute_eligible_wallets_per_wallet(
    session: requests.Session,
    agip_proposals: List[Proposal],
) -> Set[str]:
    """
    Determine which wallets are *eligible* based on AGIP6M.csv.

    Eligibility rule
    ----------------
    - A wallet (address) is eligible if *that wallet* voted on at least one of
      the proposals listed in AGIP6M.csv.

    IMPORTANT:
      - AGIP6M.csv contains main-dao proposal IDs only.
      - We do NOT apply GV2AV mirroring here.
      - We do NOT apply wallet aliasing here:
            a vote from a slave wallet makes the slave eligible, not the master.

    Parameters
    ----------
    session : requests.Session
        HTTP session object.
    agip_proposals : List[Proposal]
        Proposals loaded from AGIP6M.csv (only their IDs are used).

    Returns
    -------
    Set[str]
        Set of eligible wallet addresses (normalized).
    """
    eligible: Set[str] = set()

    total = len(agip_proposals)
    for idx, prop in enumerate(agip_proposals, start=1):
        print(f"Eligibility from AGIP6M {idx}/{total}: id={prop.id}", file=sys.stderr)
        for voter in iter_votes_for_proposal(session, prop.id):
            eligible.add(voter)

    return eligible


def write_vote_counts_csv(path: str, wallet_counts: Dict[str, int], slaves: Set[str]) -> None:
    """
    Write VoteCounts.csv:
        wallet,num_proposals

    Contains ALL identities found in counting (eligible or not).

    Safety rule:
      - If a wallet is a known slave wallet (from WalletAliases.csv),
        write num_proposals = 0 even if it appears in wallet_counts.
        (Masters already received the credit via aliasing during counting.)
    """
    # Ensure we output the union of:
    #   - wallets that appear as identities in wallet_counts
    #   - all slave wallets (so they are explicitly present as 0 if desired)
    all_wallets: Set[str] = set(wallet_counts.keys()).union(slaves)

    rows: List[Tuple[str, int]] = []
    for wallet in all_wallets:
        if wallet in slaves:
            n = 0
        else:
            n = wallet_counts.get(wallet, 0)
        rows.append((wallet, n))

    rows.sort(key=lambda x: (-x[1], x[0]))

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wallet", "num_proposals"])
        for wallet, n in rows:
            w.writerow([wallet, n])


def write_eligible_wallets_csv(path: str, eligible_wallets: Set[str]) -> None:
    """Write EligibleWalletsCached.csv: single column wallet."""
    rows = sorted(eligible_wallets)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wallet"])
        for wallet in rows:
            w.writerow([wallet])


def write_concluded_decision_count(path: str, n: int) -> None:
    """Write ConcludedDecisionCount.txt with a single integer."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(int(n)))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 8:
        print(
            "Usage:\n"
            "  python CalculateCachedVoteCounts.py IncludedProposals.csv AGIP6M.csv GV2AV.csv WalletAliases.csv "
            "VoteCounts.csv EligibleWalletsCached.csv ConcludedDecisionCount.txt",
            file=sys.stderr,
        )
        sys.exit(1)

    included_path = sys.argv[1]
    agip_path = sys.argv[2]
    gv2av_path = sys.argv[3]
    aliases_path = sys.argv[4]
    out_vote_counts = sys.argv[5]
    out_eligible_wallets = sys.argv[6]
    out_decision_count = sys.argv[7]

    included_proposals = load_proposals_from_csv(included_path)
    if not included_proposals:
        print("No proposals in IncludedProposals.csv", file=sys.stderr)
        sys.exit(1)

    # For this cached step, we assume IncludedProposals.csv are the concluded decisions we count.
    concluded_decisions = len(included_proposals)

    agip_proposals = load_proposals_from_csv(agip_path)
    if not agip_proposals:
        print("No proposals in AGIP6M.csv; EligibleWalletsCached.csv will be empty.", file=sys.stderr)

    av_to_gvs = load_av_to_gvs(gv2av_path)
    decision_groups = build_decision_groups(included_proposals, av_to_gvs)

    alias_map, masters, slaves = load_alias_map(aliases_path)
    print(f"Loaded aliases: masters={len(masters)} slaves={len(slaves)}", file=sys.stderr)
    print(f"Concluded decisions (IncludedProposals rows): {concluded_decisions}", file=sys.stderr)

    with requests.Session() as session:
        session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "snapshot-cached-vote-counts/1.0",
            }
        )

        # 1) Count decisions for ALL identities (no eligibility filtering at all).
        wallet_counts = build_decision_counts(session, decision_groups, alias_map)
        print(f"Identities with >=1 counted decision: {len(wallet_counts)}", file=sys.stderr)

        # 2) Compute eligible wallets (per-wallet, no aliasing).
        eligible_wallets = compute_eligible_wallets_per_wallet(session, agip_proposals) if agip_proposals else set()
        print(f"Eligible wallets (per wallet): {len(eligible_wallets)}", file=sys.stderr)

    # Outputs
    write_vote_counts_csv(out_vote_counts, wallet_counts, slaves)
    write_eligible_wallets_csv(out_eligible_wallets, eligible_wallets)
    write_concluded_decision_count(out_decision_count, concluded_decisions)

    print(f"Wrote: {out_vote_counts}", file=sys.stderr)
    print(f"Wrote: {out_eligible_wallets}", file=sys.stderr)
    print(f"Wrote: {out_decision_count}", file=sys.stderr)


if __name__ == "__main__":
    main()
