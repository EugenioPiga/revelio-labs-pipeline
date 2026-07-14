# Unified mobility V3 analysis

Run only:

    mobility_v3.R
    mobility_v3.sbatch

The master runner executes, in order:

1. `churn_mobility_extended.R`
2. `churn_vs_firm_characteristics.R`

Each component runs in its own R environment and writes to a separate output
subfolder. The files in `_components/` are frozen source inputs and should not
be submitted independently.

Before submission, set `REVELIO_V3_INPUT` in:

    ../00_config/v3_env.sh
