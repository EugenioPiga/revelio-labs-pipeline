# Restaurant–LinkedIn Research Sharing Package

## Final run

`owner_backbone_refresh_20260715_021137`

## Contents

This package contains:

1. The preferred exact-match dataset.
2. The exact-plus-restricted-fuzzy robustness dataset.
3. The source code and Slurm jobs used in the construction process.
4. The final linkage manifest.
5. Founder and owner counts.
6. Input-dependency documentation.
7. SHA-256 checksums.

## Preferred exact-match sample

The preferred analysis sample is `exact_baseline`.

- Matched establishments: 158,300
- Spell rows: 1,551,680
- Unique people: 1,377,586
- Founders: 2,622
- Independent owners: 19,095
- Founder or independent owner: 21,260
- Franchise owners: 1,220
- Founder or any owner: 22,455

## Exact-plus-fuzzy robustness sample

The robustness sample is `exact_plus_fuzzy`.

- Matched establishments: 174,532
- Spell rows: 1,644,914
- Unique people: 1,456,347
- Spell columns: 110
- Person columns: 61
- Founders: 2,753
- Independent owners: 20,448
- Founder or independent owner: 22,724
- Franchise owners: 1,347
- Founder or any owner: 24,044

The restricted fuzzy tier is a robustness extension and is not the preferred
matching specification.

## Chain definition

The preferred chain classification uses the supplied backbone variable
`chain_establishment_count`.

An establishment is classified as belonging to a chain when:

`chain_establishment_count >= 100`

The independently reconstructed chain measure is retained only as a diagnostic
comparison.

## Founder and owner definitions

Founder flags come from the existing restaurant spell construction.

Owner titles are classified separately. The main expanded entrepreneur
definition is:

`founder or independent owner`

Franchise owners remain a separate robustness category.

One historical spell has a documented cofounder discrepancy. The stored
historical founder classification was preserved while the new Owner fields
were added.

## Origin variable

The origin variable is inferred from LinkedIn education and employment
history. It is not verified nationality, citizenship, birthplace, or
immigration status.

## Dataset format

Each dataset is stored as a directory of Parquet files.

Python example:

    import pandas as pd

    spells = pd.read_parquet("exact_plus_fuzzy/spells")
    people = pd.read_parquet("exact_plus_fuzzy/people")

Spark example:

    spells = spark.read.parquet("exact_plus_fuzzy/spells")
    people = spark.read.parquet("exact_plus_fuzzy/people")

## Reproduction limitations

The code archive does not include all licensed or restricted source datasets.

A complete end-to-end reproduction requires authorized access to the
underlying Revelio Labs data and the restaurant backbone.
