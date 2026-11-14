"""
Run the whole NYC Taxi pipeline in order and record evidence of every run.

Steps:
  1. quality   src/data_quality.py  data quality checks on every source file
  2. curate    src/curate.py        clean + aggregate to daily trips (curated)
  3. analysis  src/analysis.py      answer the question, tables + charts

Every step is incremental: files finished in an earlier run are skipped,
so re-running only processes new months (add them in config/datasets.py)
or files that failed last time.

Usage:
  python run_pipeline.py                 run all steps
  python run_pipeline.py --force         rebuild everything from the source
  python run_pipeline.py --from curate   start from a given step
  python run_pipeline.py --only analysis run one step

Evidence written to reports/:
  pipeline_runs/run_<id>.log   full console output of the run
  pipeline_status.csv          one row per step per run (history of all runs)
"""
import argparse
import sys
import time
from datetime import datetime

import pandas as pd

from config.datasets import DATASETS, REPORTS_DIR

RUNS_DIR = REPORTS_DIR / "pipeline_runs"
STATUS_FILE = REPORTS_DIR / "pipeline_status.csv"
STEPS = ["quality", "curate", "analysis"]


class Tee:
    """Write console output to the screen and the run log at the same time."""
    def __init__(self, *streams):
        self.streams = streams

    def write(self, text):
        for s in self.streams:
            s.write(text)

    def flush(self):
        for s in self.streams:
            s.flush()


def file_status(csv_path):
    """Summarise per-file results of a step, e.g. 'SUCCESS 24/24'."""
    if not csv_path.exists():
        return "FAILED", "no result file"
    df = pd.read_csv(csv_path)
    ok = (df["status"] == "SUCCESS").sum()
    total = len(DATASETS)
    detail = f"{ok}/{total} files SUCCESS"
    failed = df.loc[df["status"] != "SUCCESS", "filename"].tolist()
    if failed:
        detail += f"; not done: {', '.join(failed)}"
    return ("SUCCESS" if ok == total else "PARTIAL"), detail


def step_quality(force):
    from src.data_quality import run_quality_checks, OUTPUT_FILE
    run_quality_checks(force=force)
    return file_status(OUTPUT_FILE)


def step_curate(force):
    from src.curate import run_curation, LOG_FILE
    run_curation(force=force)
    return file_status(LOG_FILE)


def step_analysis(force):
    from src.analysis import run_analysis
    run_analysis()
    return "SUCCESS", "answer.md, answer_summary.csv, monthly_trips.csv, 2 figures"


RUNNERS = {"quality": step_quality, "curate": step_curate, "analysis": step_analysis}


def main():
    # Windows console (e.g. cp874) cannot print some characters; avoid crashing on them
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="NYC Taxi pipeline")
    parser.add_argument("--force", action="store_true", help="rebuild everything")
    parser.add_argument("--from", dest="start", choices=STEPS, help="start from this step")
    parser.add_argument("--only", choices=STEPS, help="run only this step")
    args = parser.parse_args()

    steps = [args.only] if args.only else STEPS[STEPS.index(args.start or STEPS[0]):]

    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RUNS_DIR / f"run_{run_id}.log"
    log_file = open(log_path, "w", encoding="utf-8")
    original_stdout = sys.stdout
    sys.stdout = Tee(original_stdout, log_file)

    rows = []
    overall = "SUCCESS"
    try:
        print(f"===== PIPELINE RUN {run_id} | steps: {', '.join(steps)} | force={args.force} =====")
        for step in steps:
            print(f"\n----- STEP: {step} -----")
            started = datetime.now()
            t0 = time.time()
            try:
                status, detail = RUNNERS[step](args.force)
            except SystemExit as stop:          # analysis stops itself on incomplete data
                status, detail = "FAILED", str(stop)
            except Exception as error:
                status, detail = "FAILED", f"{type(error).__name__}: {error}"

            seconds = round(time.time() - t0, 1)
            print(f"----- {step}: {status} ({seconds}s) | {detail}")
            rows.append({
                "run_id": run_id, "step": step, "status": status,
                "started_at": started.isoformat(timespec="seconds"),
                "duration_s": seconds, "detail": detail, "force": args.force,
            })

            if status != "SUCCESS":
                overall = "PARTIAL" if status == "PARTIAL" and overall == "SUCCESS" else status
            if status == "FAILED":
                print("Stopping: later steps need this one. Re-run the same command later; "
                      "finished files will be skipped.")
                break

        print(f"\n===== RUN {run_id}: {overall} =====")
        print(f"Log: {log_path}")
        print(f"Status history: {STATUS_FILE}")
    finally:
        sys.stdout = original_stdout
        log_file.close()
        history = pd.DataFrame(rows)
        if STATUS_FILE.exists():
            history = pd.concat([pd.read_csv(STATUS_FILE), history], ignore_index=True)
        history.to_csv(STATUS_FILE, index=False)

    sys.exit(0 if overall == "SUCCESS" else 1)


if __name__ == "__main__":
    main()