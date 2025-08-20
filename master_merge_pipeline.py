#!/usr/bin/env python
# master_merge_pipeline.py
#
# Runs the full pipeline end-to-end (Parquet-only; no CSV/JSON artifacts):
#   1) step1_inventors_matched_users.py
#   2) step2_inventors_matched_positions.py
#   3) step3_inventors_matched_education.py
#
# Args: forwards per-step limits, partition sizes, and required input dirs.

import os
import sys
import argparse
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent

# ---- Script paths (override with CLI if needed)
STEP1_SCRIPT = HERE / "step1_inventors_matched_users.py"
STEP2_SCRIPT = HERE / "step2_inventors_matched_positions.py"
STEP3_SCRIPT = HERE / "step3_inventors_matched_education.py"

# ---- Expected outputs
STEP1_OUT = HERE / "output" / "inventors_matched_users"
STEP2_OUT = HERE / "output" / "inventors_matched_positions"
STEP3_OUT = HERE / "output" / "inventors_matched_education"

def run(cmd: list[str], title: str, extra_env: dict | None = None) -> None:
    """Run a subprocess, streaming output; raise on non-zero exit."""
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)
    print(f"\n===== {title} =====")
    print("Command:", " ".join(cmd))
    print("=" * 60, flush=True)
    proc = subprocess.run(cmd, env=env)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)
    print(f"===== {title} DONE (exit=0) =====\n", flush=True)

def check_parquet_dir(p: Path, name: str) -> None:
    """Basic sanity: output directory exists and has at least 1 parquet part."""
    if not p.exists():
        raise SystemExit(f"[ERROR] Expected output directory for {name} not found: {p}")
    parts = list(p.glob("*.parquet")) + list(p.glob("part*.parquet"))
    if not parts:
        parts = list(p.rglob("*.parquet"))
    if not parts:
        raise SystemExit(f"[ERROR] No parquet files detected in {p} for {name}. Check logs.")

def main():
    ap = argparse.ArgumentParser(description="Run the inventor merge pipeline end-to-end (Parquet-only).")

    # Step 1 limits
    ap.add_argument("--limit-users", type=int, default=None,
                    help="Step 1: max number of USER parquet files to read.")
    ap.add_argument("--limit-inv", type=int, default=None,
                    help="Step 1: max number of INVENTOR parquet files to read.")

    # Step 2 inputs & limits
    ap.add_argument("--positions-dir", default="academic_individual_position",
                    help="Step 2: directory or glob for positions parquet (e.g., '.../*.parquet').")
    ap.add_argument("--limit-pos", type=int, default=None,
                    help="Step 2: max number of POSITIONS parquet files to read (if script supports it).")

    # Step 3 inputs & limits
    ap.add_argument("--education-dir", default="academic_individual_user_education",
                    help="Step 3: root directory or glob for education parquet.")
    ap.add_argument("--limit-edu", type=int, default=None,
                    help="Step 3: max number of EDUCATION parquet files to read.")

    # Partition sizes
    ap.add_argument("--part-size-1", default="128MB", help="Step 1 output partition size.")
    ap.add_argument("--part-size-2", default="128MB", help="Step 2 output partition size.")
    ap.add_argument("--part-size-3", default="128MB", help="Step 3 output partition size.")

    # Optional step toggles
    ap.add_argument("--skip-1", action="store_true", help="Skip Step 1 (expects prior output).")
    ap.add_argument("--skip-2", action="store_true", help="Skip Step 2 (expects prior output).")
    ap.add_argument("--skip-3", action="store_true", help="Skip Step 3.")

    # Override script paths
    ap.add_argument("--step1-script", default=str(STEP1_SCRIPT))
    ap.add_argument("--step2-script", default=str(STEP2_SCRIPT))
    ap.add_argument("--step3-script", default=str(STEP3_SCRIPT))

    args = ap.parse_args()
    py = sys.executable

    # Favor faster Dask shuffle unless explicitly set
    extra_env = {}
    if "DASK_DISTRIBUTED__SHUFFLE__METHOD" not in os.environ:
        extra_env["DASK_DISTRIBUTED__SHUFFLE__METHOD"] = "p2p"

    # ---- STEP 1 ----
    if not args.skip_1:
        step1_cmd = [py, "-u", args.step1_script, "--part-size", args.part_size_1]
        if args.limit_users is not None:
            step1_cmd += ["--limit-users", str(args.limit_users)]
        if args.limit_inv is not None:
            step1_cmd += ["--limit-inv", str(args.limit_inv)]
        run(step1_cmd, "STEP 1: Inventor ∩ User", extra_env)
    check_parquet_dir(STEP1_OUT, "Step 1")

    # ---- STEP 2 ----
    if not args.skip_2:
        # Normalize positions path to accept either a directory or a glob
        pos_arg = args.positions_dir
        if Path(pos_arg).is_dir():
            # Let the step2 script expand to *.parquet; if yours requires explicit glob, append here:
            # pos_arg = str(Path(pos_arg) / "*.parquet")
            pos_arg = pos_arg
        step2_cmd = [
            py, "-u", args.step2_script,
            "--step1-dir", str(STEP1_OUT),
            "--positions-dir", pos_arg,
            "--part-size", args.part_size_2
        ]
        if args.limit_pos is not None:
            step2_cmd += ["--limit-pos", str(args.limit_pos)]
        run(step2_cmd, "STEP 2: Add has_position + join positions", extra_env)
    check_parquet_dir(STEP2_OUT, "Step 2")

    # ---- STEP 3 ----
    if not args.skip_3:
        edu_arg = args.education_dir
        if Path(edu_arg).is_dir():
            edu_arg = edu_arg
        step3_cmd = [
            py, "-u", args.step3_script,
            "--step2-dir", str(STEP2_OUT),
            "--education-dir", edu_arg,
            "--part-size", args.part_size_3
        ]
        if args.limit_edu is not None:
            step3_cmd += ["--limit-edu", str(args.limit_edu)]
        run(step3_cmd, "STEP 3: Add has_education + join education", extra_env)
    check_parquet_dir(STEP3_OUT, "Step 3")

    print("\nAll steps completed successfully ✅")
    print(f"Step 1 parquet: {STEP1_OUT}")
    print(f"Step 2 parquet: {STEP2_OUT}")
    print(f"Step 3 parquet (final): {STEP3_OUT}")

if __name__ == "__main__":
    main()
