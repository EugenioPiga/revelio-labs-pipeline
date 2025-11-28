"""Analyze rcid and ultimate parent coverage for US inventors in 2018."""

from pathlib import Path

import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.expand_frame_repr", False)
INVENTOR_DIR = Path("/labs/khanna/linkedin_202507/processed/inventor_year_merged")
CROSSWALK_PATH = Path(
    "/home/gps-yuhei/code/revelio-labs-pipeline/data/rcid_parent_crosswalk.parquet"
)
FILTER_COUNTRY = "United States"
FILTER_YEAR = 2018
TOP_N = 50
ATTRIBUTE_COLUMNS = [
    "rcid_sedol",
    "rcid_ticker",
    "rcid_gvkey",
    "rcid_isin",
    "rcid_cusip",
    "rcid_cik",
    "rcid_lei",
    "rcid_naics_code",
    "rcid_factset_entity_id",
]


def load_inventor_parquets(data_dir: Path = INVENTOR_DIR) -> pd.DataFrame:
    """Load and combine every inventor-year parquet file."""
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")
    frames = [pd.read_parquet(file) for file in parquet_files]
    return pd.concat(frames, ignore_index=True)


def filter_us_2018(df: pd.DataFrame) -> pd.DataFrame:
    """Restrict to US inventors in the target year."""
    mask = (df["first_country"] == FILTER_COUNTRY) & (df["year"] == FILTER_YEAR)
    return df.loc[mask].copy()


def merge_with_crosswalk(df: pd.DataFrame) -> pd.DataFrame:
    """Append parent rcid information."""
    crosswalk = pd.read_parquet(CROSSWALK_PATH)
    return df.merge(
        crosswalk, left_on="first_rcid", right_on="rcid", how="left", suffixes=("", "_cw")
    )


def summarize_by_id(
    df: pd.DataFrame,
    id_col: str,
    name_col: str,
    parent_id_col: str | None = None,
    parent_name_col: str | None = None,
) -> pd.DataFrame:
    """Aggregate observation counts, geographies, and names by the provided ID."""
    agg = (
        df.groupby(id_col)
        .agg(
            name=pd.NamedAgg(
                column=name_col,
                aggfunc=lambda s: s.dropna().iloc[0] if s.notna().any() else None,
            ),
            observations=pd.NamedAgg(column="first_rcid", aggfunc="size"),
            unique_states=pd.NamedAgg(column="first_state", aggfunc="nunique"),
            unique_metros=pd.NamedAgg(column="first_metro_area", aggfunc="nunique"),
        )
        .sort_values("observations", ascending=False)
        .head(TOP_N)
    )
    if parent_id_col:
        agg["ultimate_parent_rcid"] = df.groupby(id_col)[parent_id_col].first()
    if parent_name_col:
        agg["ultimate_parent_name"] = df.groupby(id_col)[parent_name_col].apply(
            lambda s: s.dropna().iloc[0] if s.notna().any() else None
        )
    return agg


def report_missing_fractions(df: pd.DataFrame) -> None:
    """Print share of missing values for each attribute, weighted by inventors."""
    total = len(df)
    print("\nAttribute missingness (weighted by inventor count):")
    for column in ATTRIBUTE_COLUMNS:
        if column not in df.columns:
            continue
        missing_fraction = df[column].isna().sum() / total
        print(f"- {column}: {missing_fraction:.2%} missing")


def main() -> None:
    inventor_df = load_inventor_parquets()
    filtered = filter_us_2018(inventor_df)
    merged = merge_with_crosswalk(filtered)

    print(
        f"Found {len(filtered):,} US inventor observations for {FILTER_YEAR} "
        f"in {INVENTOR_DIR}"
    )

    print("\nTop rcid entities:")
    rcid_summary = summarize_by_id(
        merged,
        "first_rcid",
        "rcid_name",
        parent_id_col="ultimate_parent_rcid_new",
        parent_name_col="ultimate_parent_rcid_name",
    )
    print(rcid_summary)

    print("\nTop ultimate parent entities:")
    parent_summary = summarize_by_id(
        merged, "ultimate_parent_rcid_new", "ultimate_parent_rcid_name"
    )
    print(parent_summary)

    report_missing_fractions(merged)


if __name__ == "__main__":
    main()
