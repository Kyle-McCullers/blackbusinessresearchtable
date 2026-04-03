import re
import uuid
import warnings

from rapidfuzz import fuzz

_LEGAL_SUFFIXES = re.compile(
    r"\b(llc|inc|corp|co|ltd|company|enterprises?|services?|group|associates?|solutions?)\b\.?",
    re.IGNORECASE,
)
_NON_ALNUM = re.compile(r"[^a-z0-9\s]")
_WHITESPACE = re.compile(r"\s+")


def normalize_name(name: str) -> str:
    """Lowercase, strip legal suffixes and punctuation, collapse whitespace."""
    name = name.lower().strip()
    name = _LEGAL_SUFFIXES.sub(" ", name)
    name = _NON_ALNUM.sub(" ", name)
    return _WHITESPACE.sub(" ", name).strip()


def normalize_zip(zip_code: str) -> str:
    """Return first 5 digits, zero-padded. Returns '' if no digits found."""
    digits = re.sub(r"[^0-9]", "", str(zip_code))[:5]
    return digits.zfill(5) if digits else ""


def resolve(
    new_records: list[dict],
    registry: list[dict],
    snapshot_id: str,
    review_log: list[dict],
) -> tuple[list[dict], list[dict]]:
    """
    Assign stable business_ids to new_records by matching against registry.

    Match priority:
      1. (source_id, source_business_id) exact match
      2. (source_id, canonical_name, canonical_zip) exact match
      3. (source_id, canonical_zip) + fuzzy name similarity >= 95%
      4. Near-miss (80–94%) logged to review_log but treated as new entity

    Returns:
      (augmented_records, new_registry_entries)

    Note: review_log is mutated in place with near-miss entries (80–94% similarity).
    """
    # Build O(1) lookups
    by_src_biz_id: dict[tuple, str] = {}   # (source_id, source_business_id) -> business_id
    by_name_zip: dict[tuple, str] = {}      # (source_id, canonical_name, canonical_zip) -> business_id

    for entry in registry:
        src_id = entry["source_id"]
        src_biz_id = entry.get("source_business_id", "")
        if src_biz_id:
            by_src_biz_id[(src_id, src_biz_id)] = entry["business_id"]
        by_name_zip[(src_id, entry["canonical_name"], entry["canonical_zip"])] = entry["business_id"]

    # Group registry entries by (source_id, canonical_zip) for fuzzy fallback
    by_zip: dict[tuple, list[dict]] = {}
    for entry in registry:
        key = (entry["source_id"], entry["canonical_zip"])
        by_zip.setdefault(key, []).append(entry)

    result = []
    new_entries = []

    for rec in new_records:
        source_id = rec.get("source_id", "")
        if not source_id:
            warnings.warn(f"Skipping record with missing source_id: {rec.get('business_name')!r}")
            continue
        src_biz_id = rec.get("source_business_id", "")
        can_name = normalize_name(rec.get("business_name", ""))
        can_zip = normalize_zip(rec.get("address_zip", ""))

        business_id = None

        # Priority 1: source_business_id exact match (within same source)
        if src_biz_id:
            business_id = by_src_biz_id.get((source_id, src_biz_id))

        # Priority 2: canonical name + zip exact match (within same source)
        if not business_id and can_name:
            business_id = by_name_zip.get((source_id, can_name, can_zip))

        # Priority 3: fuzzy name match within same source and zip
        if not business_id and can_name and can_zip:
            candidates = by_zip.get((source_id, can_zip), [])
            best_score = 0
            best_entry = None
            for entry in candidates:
                score = fuzz.token_sort_ratio(can_name, entry["canonical_name"])
                if score > best_score:
                    best_score = score
                    best_entry = entry
            if best_score >= 95 and best_entry:
                business_id = best_entry["business_id"]
            elif best_score >= 80 and best_entry:
                review_log.append({
                    "snapshot_id": snapshot_id,
                    "new_name": rec.get("business_name"),
                    "new_canonical": can_name,
                    "matched_name": best_entry["canonical_name"],
                    "zip": can_zip,
                    "source_id": source_id,
                    "similarity": best_score,
                    "candidate_id": best_entry["business_id"],
                })

        # Priority 4: new entity
        if not business_id:
            business_id = str(uuid.uuid4())
            new_entries.append({
                "business_id": business_id,
                "canonical_name": can_name,
                "canonical_zip": can_zip,
                "source_id": source_id,
                "source_business_id": src_biz_id,
            })

        result.append({
            **rec,
            "business_id": business_id,
            "canonical_name": can_name,
            "canonical_zip": can_zip,
        })

    return result, new_entries
