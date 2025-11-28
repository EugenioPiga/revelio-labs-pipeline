"""Inspect rcid to ultimate parent mapping using the first parquet file."""

from pathlib import Path

import pandas as pd

DATA_DIR = Path("/labs/khanna/linkedin_202507/academic_company_ref")
TARGET_COLUMNS = [
    "ultimate_parent_rcid",
    "ultimate_parent_rcid_name",
    "rcid",
    "company",
]
MAPPING_COLUMNS = ["ultimate_parent_rcid", "rcid"]


def load_first_parquet(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load only the first parquet file in the directory."""
    first_file = next(iter(sorted(data_dir.glob("*.parquet"))), None)
    if first_file is None:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")
    df = pd.read_parquet(first_file, columns=TARGET_COLUMNS)
    df["ultimate_parent_rcid"] = df["ultimate_parent_rcid"].fillna(df["rcid"])
    return df


def extract_unique_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Return unique rcid-to-ultimate-parent mappings."""
    return df[MAPPING_COLUMNS].drop_duplicates().reset_index(drop=True)


def build_name_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """Create a unique rcid-to-name lookup table."""
    child_names = (
        df[["rcid", "company"]]
        .dropna(subset=["rcid"])
        .drop_duplicates(subset="rcid")
        .rename(columns={"company": "name"})
    )
    parent_names = (
        df[["ultimate_parent_rcid", "ultimate_parent_rcid_name"]]
        .dropna(subset=["ultimate_parent_rcid"])
        .drop_duplicates(subset="ultimate_parent_rcid")
        .rename(
            columns={
                "ultimate_parent_rcid": "rcid",
                "ultimate_parent_rcid_name": "name",
            }
        )
    )
    combined = pd.concat([child_names, parent_names], ignore_index=True)
    return combined.drop_duplicates(subset="rcid", keep="first")


def rcid_has_unique_parent(mapping: pd.DataFrame) -> pd.DataFrame:
    """Return rows where the same rcid maps to multiple parents."""
    duplicates = mapping.duplicated(subset="rcid", keep=False)
    return mapping.loc[duplicates].sort_values("rcid")


def summarize_mapping(mapping: pd.DataFrame) -> dict:
    """Compute summary metrics for the mapping dataset."""
    mismatch_count = (mapping["ultimate_parent_rcid"] != mapping["rcid"]).sum()
    return {
        "total_rows": len(mapping),
        "ultimate_parent_not_self": mismatch_count,
        "unique_ultimate_parents": mapping["ultimate_parent_rcid"].nunique(),
    }


def resolve_to_top_parent(mapping: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Iteratively climb the rcid tree until parents no longer change."""
    resolved = mapping.copy()
    resolved["ultimate_parent_rcid_new"] = resolved["ultimate_parent_rcid"]
    iterations = 0

    while True:
        mask = resolved["ultimate_parent_rcid_new"] != resolved["rcid"]
        if not mask.any():
            break

        parent_lookup = (
            resolved.set_index("rcid")["ultimate_parent_rcid_new"].to_dict()
        )
        candidates = resolved.loc[mask, "ultimate_parent_rcid_new"]
        updated_parents = candidates.map(lambda parent: parent_lookup.get(parent, parent))

        if updated_parents.equals(candidates):
            break

        resolved.loc[candidates.index, "ultimate_parent_rcid_new"] = updated_parents
        iterations += 1

    return resolved, iterations


def attach_entity_names(
    resolved_mapping: pd.DataFrame, name_lookup: pd.DataFrame
) -> pd.DataFrame:
    """Attach company names for rcid and ultimate parent rcid."""
    child_names = name_lookup.rename(columns={"name": "rcid_name"})
    parent_names = name_lookup.rename(
        columns={"rcid": "ultimate_parent_rcid_new", "name": "ultimate_parent_rcid_name"}
    )
    enriched = resolved_mapping.merge(child_names, on="rcid", how="left")
    return enriched.merge(parent_names, on="ultimate_parent_rcid_new", how="left")


def report_top_parent_groups(
    enriched_mapping: pd.DataFrame, limit: int = 10, child_preview: int = 10
) -> None:
    """Print top parent groups with their rcids and company names."""
    parent_counts = (
        enriched_mapping.groupby(
            ["ultimate_parent_rcid_new", "ultimate_parent_rcid_name"], dropna=False
        )["rcid"]
        .nunique()
        .reset_index(name="child_count")
        .sort_values("child_count", ascending=False)
        .head(limit)
    )

    print("\nStep 6: Top ultimate parent groups (by rcid count)")
    for _, row in parent_counts.iterrows():
        parent_id = row["ultimate_parent_rcid_new"]
        parent_name = row.get("ultimate_parent_rcid_name") or "Unknown"
        child_count = row["child_count"]
        print(
            f"\nUltimate parent {parent_id} ({parent_name}) - {child_count} rcid(s)"
        )
        children = (
            enriched_mapping.loc[
                enriched_mapping["ultimate_parent_rcid_new"] == parent_id,
                ["rcid", "rcid_name"],
            ]
            .sort_values("rcid")
            .head(child_preview)
        )
        print(children.to_string(index=False))


def main() -> None:
    df = load_first_parquet()
    mapping = extract_unique_mapping(df)
    name_lookup = build_name_lookup(df)
    duplicates = rcid_has_unique_parent(mapping)
    summary = summarize_mapping(mapping)
    resolved_mapping, iterations = resolve_to_top_parent(mapping)
    enriched_mapping = attach_entity_names(resolved_mapping, name_lookup)

    print(
        f"Loaded {len(df):,} rows from first parquet in {DATA_DIR} and found "
        f"{len(mapping):,} unique rcid mappings."
    )

    print("\nStep 2: rcid uniqueness check")
    if duplicates.empty:
        print("All rcid values map to a single ultimate_parent_rcid.")
    else:
        print("Found rcid values with multiple ultimate_parent_rcid mappings:")
        print(duplicates.to_string(index=False))

    print("\nStep 3: Summary metrics")
    for key, value in summary.items():
        print(f"{key}: {value:,}")

    print("\nStep 4 & 5: Cascading ultimate parent mapping")
    updated_rows = (
        resolved_mapping["ultimate_parent_rcid_new"]
        != resolved_mapping["ultimate_parent_rcid"]
    ).sum()
    print(f"Iterations executed: {iterations}")
    print(f"Rows updated to higher-level parents: {updated_rows:,}")

    if updated_rows:
        preview_cols = ["rcid", "ultimate_parent_rcid", "ultimate_parent_rcid_new"]
        print("Sample of updated mappings:")
        print(
            enriched_mapping.loc[
                resolved_mapping["ultimate_parent_rcid_new"]
                != resolved_mapping["ultimate_parent_rcid"],
                preview_cols,
            ]
            .head(10)
            .to_string(index=False)
        )

    report_top_parent_groups(enriched_mapping)


if __name__ == "__main__":
    main()
