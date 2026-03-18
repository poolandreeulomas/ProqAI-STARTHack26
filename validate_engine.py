"""
validate_engine.py — ChainIQ START Hack 2026
=============================================
Runs SupplierEngine over all requests, then compares each result against
historical_awards.csv on four dimensions:

  1. winner_match        — our rank-1 supplier == historically awarded supplier
  2. winner_in_shortlist — historically awarded supplier appears anywhere in our shortlist
  3. escalation_match    — we flagged ≥1 escalation iff history says escalation_required=True
  4. status_vs_awarded   — if history awarded a supplier, our status should not be cannot_proceed

Outputs
-------
  - Console: summary table + full disagreement list grouped by type
  - validate_report.json: machine-readable version of the same
"""
from __future__ import annotations

import json
from collections import defaultdict
from csv import DictReader
from pathlib import Path

from supplier_engine import SupplierEngine, _load_json, DATA_DIR


# ---------------------------------------------------------------------------
# Load & index historical awards
# ---------------------------------------------------------------------------

def load_awards(data_dir: Path) -> dict[str, list[dict]]:
    """Returns {request_id: [award_rows]} — one request may have 1–3 rows."""
    with open(data_dir / "historical_awards.csv", encoding="utf-8") as f:
        rows = list(DictReader(f))
    index: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        index[row["request_id"]].append(row)
    return dict(index)


def awarded_row(rows: list[dict]) -> dict | None:
    """Return the single row where awarded == 'True', or None."""
    winners = [r for r in rows if r.get("awarded", "").strip().lower() == "true"]
    return winners[0] if winners else None


# ---------------------------------------------------------------------------
# Per-request comparison
# ---------------------------------------------------------------------------

TERMINAL_STATUSES = {"cannot_proceed", "no_eligible_suppliers"}


def compare(result: dict, award_rows: list[dict]) -> dict:
    shortlist  = result.get("supplier_shortlist", [])
    escalations = result.get("escalations", [])
    status     = result["recommendation"]["status"]

    winner      = awarded_row(award_rows)
    hist_sup_id   = winner["supplier_id"]   if winner else None
    hist_sup_name = winner["supplier_name"] if winner else None
    hist_escalated = (
        winner["escalation_required"].strip().lower() == "true" if winner else None
    )
    hist_escalated_to = winner.get("escalated_to", "").strip() if winner else ""
    hist_total    = float(winner["total_value"]) if winner else None
    hist_currency = winner.get("currency", "")  if winner else ""

    our_rank1    = shortlist[0] if shortlist else None
    our_rank1_id = our_rank1["supplier_id"] if our_rank1 else None
    our_escalated = len(escalations) > 0
    shortlist_ids = [s["supplier_id"] for s in shortlist]

    disagreements: list[str] = []

    # ── 1. Winner match ──────────────────────────────────────────────────────
    if winner and our_rank1:
        winner_match = our_rank1_id == hist_sup_id
        if not winner_match:
            if hist_sup_id in shortlist_ids:
                hist_rank = next(i + 1 for i, s in enumerate(shortlist) if s["supplier_id"] == hist_sup_id)
                disagreements.append(
                    f"[WINNER] We rank '{our_rank1['supplier_name']}' #1; "
                    f"history awarded '{hist_sup_name}' (found at our rank {hist_rank})."
                )
            else:
                disagreements.append(
                    f"[WINNER] We rank '{our_rank1['supplier_name']}' #1; "
                    f"history awarded '{hist_sup_name}' — not in our shortlist at all."
                )
    else:
        winner_match = None

    # ── 2. Winner in shortlist ───────────────────────────────────────────────
    if winner:
        winner_in_shortlist = hist_sup_id in shortlist_ids
        if not winner_in_shortlist:
            disagreements.append(
                f"[COVERAGE] Historically awarded '{hist_sup_name}' ({hist_sup_id}) "
                f"is absent from our shortlist entirely."
            )
    else:
        winner_in_shortlist = None

    # ── 3. Escalation match ──────────────────────────────────────────────────
    if hist_escalated is not None:
        escalation_match = our_escalated == hist_escalated
        if not escalation_match:
            our_targets = ", ".join(e["escalate_to"] for e in escalations) or "none"
            if hist_escalated:
                disagreements.append(
                    f"[ESCALATION] History escalated to '{hist_escalated_to}' "
                    f"but we raised no escalations."
                )
            else:
                disagreements.append(
                    f"[ESCALATION] We escalated to [{our_targets}] "
                    f"but history shows no escalation required."
                )
    else:
        escalation_match = None

    # ── 4. Status vs awarded ─────────────────────────────────────────────────
    if winner:
        status_ok = status not in TERMINAL_STATUSES
        if not status_ok:
            disagreements.append(
                f"[STATUS] History awarded '{hist_sup_name}' but our status is '{status}' "
                f"(we consider this request infeasible or missing required input)."
            )
    else:
        status_ok = None

    # ── 5. Price proximity (informational — not a hard pass/fail) ────────────
    price_note = None
    if winner and our_rank1 and hist_total:
        our_total = our_rank1.get("total_price_eur")
        if our_total:
            diff_pct = abs(our_total - hist_total) / hist_total * 100
            if diff_pct > 15:
                price_note = (
                    f"Price gap >15%: our rank-1 {hist_currency} {our_total:,.2f} "
                    f"vs historical {hist_currency} {hist_total:,.2f} ({diff_pct:.1f}% diff)."
                )

    return {
        "request_id": result["request_id"],
        "our_status": status,
        "our_rank1_supplier": our_rank1["supplier_name"] if our_rank1 else None,
        "hist_awarded_supplier": hist_sup_name,
        "checks": {
            "winner_match":        winner_match,
            "winner_in_shortlist": winner_in_shortlist,
            "escalation_match":    escalation_match,
            "status_vs_awarded":   status_ok,
        },
        "price_note": price_note,
        "disagreements": disagreements,
        "all_pass": len(disagreements) == 0,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_validation(data_dir: Path = DATA_DIR) -> None:
    print("Loading data and running engine on all requests...")
    engine   = SupplierEngine(data_dir)
    requests = _load_json(data_dir / "requests.json")
    awards   = load_awards(data_dir)

    results_by_id = {r["request_id"]: engine.process(r) for r in requests}

    comparable_ids = sorted(set(results_by_id) & set(awards))
    engine_only    = sorted(set(results_by_id) - set(awards))

    print(f"\n  Requests processed      : {len(results_by_id)}")
    print(f"  Historical award records: {len(awards)}")
    print(f"  Comparable (overlap)    : {len(comparable_ids)}")
    print(f"  Engine-only (no history): {len(engine_only)}")

    comparisons = [
        compare(results_by_id[rid], awards[rid])
        for rid in comparable_ids
    ]

    # ── Agreement rates ───────────────────────────────────────────────────────
    def rate(key: str) -> tuple[int, int, str]:
        applicable = [c for c in comparisons if c["checks"][key] is not None]
        passed     = [c for c in applicable  if c["checks"][key] is True]
        n, d = len(passed), len(applicable)
        return n, d, f"{n}/{d} ({100*n/d:.1f}%)" if d else "N/A"

    print("\n" + "=" * 62)
    print("VALIDATION SUMMARY")
    print("=" * 62)
    checks = [
        ("winner_match",        "Rank-1 == awarded supplier"),
        ("winner_in_shortlist", "Awarded supplier in our shortlist"),
        ("escalation_match",    "Escalation flag agrees with history"),
        ("status_vs_awarded",   "Not blocked when history awarded"),
    ]
    for key, label in checks:
        _, _, pct = rate(key)
        print(f"  {label:<40} {pct}")

    all_pass_count = sum(1 for c in comparisons if c["all_pass"])
    total = len(comparisons)
    print(f"\n  Fully agreeing requests: {all_pass_count}/{total} ({100*all_pass_count/total:.1f}%)")

    # ── Disagreements grouped by type ────────────────────────────────────────
    disagreeing = [c for c in comparisons if not c["all_pass"]]
    print(f"\n{'=' * 62}")
    print(f"DISAGREEMENTS  ({len(disagreeing)} requests)")
    print("=" * 62)

    by_type: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for c in disagreeing:
        for note in c["disagreements"]:
            tag = note.split("]")[0].lstrip("[")
            by_type[tag].append((c["request_id"], note))

    req_lookup = {r["request_id"]: r for r in requests}

    for tag, items in sorted(by_type.items()):
        print(f"\n── {tag}  ({len(items)}) " + "─" * max(0, 50 - len(tag)))
        for rid, note in items:
            rec   = results_by_id[rid]["recommendation"]
            req   = req_lookup[rid]
            label = f"{req['category_l2']} / {req['country']}"
            print(f"  {rid}  [{label}]  status={rec['status']}")
            print(f"    {note}")
            if rec.get("minimum_budget_required"):
                print(f"    → min_budget_required = {rec['minimum_budget_required']}")
            if rec.get("clarifications_needed"):
                for cl in rec["clarifications_needed"]:
                    print(f"    → clarification: {cl['field'][:80]}")

    # ── Price notes ───────────────────────────────────────────────────────────
    price_flagged = [(c["request_id"], c["price_note"]) for c in comparisons if c["price_note"]]
    if price_flagged:
        print(f"\n── PRICE GAP >15%  ({len(price_flagged)}) " + "─" * 28)
        for rid, note in price_flagged:
            req = req_lookup[rid]
            print(f"  {rid}  [{req['category_l2']} / {req['country']}]")
            print(f"    {note}")

    # ── Write JSON report ─────────────────────────────────────────────────────
    report = {
        "summary": {
            "total_processed": len(results_by_id),
            "historical_award_records": len(awards),
            "comparable": len(comparable_ids),
            "engine_only": len(engine_only),
            "agreement_rates": {key: rate(key)[2] for key, _ in checks},
            "fully_agreeing": all_pass_count,
            "fully_agreeing_pct": round(100 * all_pass_count / total, 1),
        },
        "disagreements": [c for c in comparisons if not c["all_pass"]],
        "all_comparisons": comparisons,
    }
    out = Path(__file__).parent / "validate_report.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nFull report → {out}")


if __name__ == "__main__":
    run_validation()
