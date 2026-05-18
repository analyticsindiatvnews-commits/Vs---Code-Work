#!/usr/bin/env python3
"""
===============================================================
  AWS S3 Daily Sync Scheduler
  Syncs yesterday's S3 logs folder to local disk every day.
===============================================================
"""

# ============================================================
#  CONFIGURATION — Edit these values once, leave rest alone
# ============================================================
RUN_TIME        = "09:30"                                    # 24-hr HH:MM
S3_BUCKET       = "veto-stream-logs"
S3_PREFIX       = "veto-stream-logs"                         # path inside bucket before /MM/DD
LOCAL_BASE      = r"D:\Veto Logs Backup\Veto Logs"           # base folder; day subfolder added auto
ENDPOINT_URL    = "https://in-maa-1.linodeobjects.com"
EXTRA_AWS_ARGS  = ["--progress-multiline", "--size-only"]    # extra aws s3 sync flags
# ============================================================

import datetime
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = Path(__file__).parent.resolve()
LOG_DIR     = SCRIPT_DIR / "sync_logs"
STATE_FILE  = SCRIPT_DIR / ".last_run_date"   # hidden file tracks last successful run date
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging helpers ───────────────────────────────────────────────────────────
_log_handler_file = None   # keep reference so we can swap the file each day

def _daily_log_path() -> Path:
    return LOG_DIR / f"sync_{datetime.date.today().strftime('%Y-%m-%d')}.log"

def setup_logging():
    """Initialise (or refresh) logging so messages go to today's log file."""
    global _log_handler_file
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Remove stale handlers
    for h in root.handlers[:]:
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass

    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")

    _log_handler_file = logging.FileHandler(_daily_log_path(), encoding="utf-8")
    _log_handler_file.setFormatter(fmt)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)

    root.addHandler(_log_handler_file)
    root.addHandler(console)

# ── State helpers ──────────────────────────────────────────────────────────────
def _today_iso() -> str:
    return datetime.date.today().isoformat()

def has_run_today() -> bool:
    try:
        return STATE_FILE.read_text().strip() == _today_iso()
    except FileNotFoundError:
        return False

def mark_ran_today():
    STATE_FILE.write_text(_today_iso())

# ── Core sync job ──────────────────────────────────────────────────────────────
def run_sync():
    """Build and execute the aws s3 sync command for yesterday's folder."""
    setup_logging()   # refresh to today's file in case we crossed midnight

    if has_run_today():
        logging.info("Sync already completed today — skipping duplicate run.")
        return

    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    month_str = yesterday.strftime("%m")
    day_str   = yesterday.strftime("%d")

    s3_source   = f"s3://{S3_BUCKET}/{S3_PREFIX}/{month_str}/{day_str}"
    local_dest  = str(Path(LOCAL_BASE) / day_str)

    # Create local destination directory if missing
    try:
        Path(local_dest).mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logging.error(f"Cannot create local folder {local_dest!r}: {exc}")
        return

    cmd = (
        ["aws", "s3", "sync", s3_source, local_dest,
         "--endpoint-url", ENDPOINT_URL]
        + EXTRA_AWS_ARGS
    )

    sep = "=" * 64
    logging.info(sep)
    logging.info("SYNC_STARTED")
    logging.info(f"  Syncing data for : {yesterday.strftime('%d/%m/%Y')}  (yesterday)")
    logging.info(f"  S3 source        : {s3_source}")
    logging.info(f"  Local dest       : {local_dest}")
    logging.info(f"  Command          : {' '.join(cmd)}")
    logging.info(sep)

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for line in proc.stdout:
            line = line.rstrip()
            if line:
                logging.info(line)
        proc.wait()

        if proc.returncode == 0:
            logging.info(sep)
            logging.info("SYNC_COMPLETED — SUCCESS ✓")
            logging.info(sep)
            mark_ran_today()
        else:
            logging.error(sep)
            logging.error(f"SYNC_FAILED — exit code {proc.returncode}")
            logging.error(sep)

    except FileNotFoundError:
        logging.error("SYNC_ERROR — 'aws' command not found. "
                      "Is AWS CLI installed and added to PATH?")
    except Exception as exc:
        logging.error(f"SYNC_ERROR — Unexpected error: {exc}")

# ── Startup catch-up check ─────────────────────────────────────────────────────
def startup_check():
    """
    Called once when the script starts.
    • Logs that the scheduler is alive (visible proof after every PC restart).
    • If we're past today's RUN_TIME and the job hasn't run yet, runs it now
      (covers the case where the PC was off at 09:30 and booted later).
    """
    setup_logging()

    sep = "=" * 64
    logging.info(sep)
    logging.info("SCHEDULER STARTED  (script is now running)")
    logging.info(f"  Script   : {__file__}")
    logging.info(f"  Run time : {RUN_TIME} daily")
    logging.info(f"  Today    : {_today_iso()}")
    logging.info(sep)

    # Tell user whether today's sync already happened
    if has_run_today():
        logging.info("Today's sync already completed before this start. "
                     "Waiting for tomorrow's scheduled run.")
        return

    # Check if we're past today's scheduled time
    now = datetime.datetime.now()
    run_h, run_m = map(int, RUN_TIME.split(":"))
    target_today = now.replace(hour=run_h, minute=run_m, second=0, microsecond=0)

    if now >= target_today:
        logging.warning("PC/script started AFTER scheduled time and sync has NOT run yet "
                        "today — running missed sync NOW.")
        run_sync()
    else:
        mins_left = int((target_today - now).total_seconds() / 60)
        logging.info(f"Sync not due yet. Will run at {RUN_TIME} "
                     f"(about {mins_left} minute(s) from now).")

# ── Simple time-loop scheduler (no 3rd-party libs needed) ─────────────────────
def _next_run_datetime() -> datetime.datetime:
    run_h, run_m = map(int, RUN_TIME.split(":"))
    now   = datetime.datetime.now()
    today = now.replace(hour=run_h, minute=run_m, second=0, microsecond=0)
    if now < today:
        return today
    # Already past today's time — schedule for tomorrow
    return today + datetime.timedelta(days=1)

def main():
    startup_check()

    next_run = _next_run_datetime()
    logging.info(f"Next scheduled sync: {next_run.strftime('%Y-%m-%d %H:%M')}")

    while True:
        now = datetime.datetime.now()

        # Swap log file at midnight (day rolled over)
        setup_logging()

        if now >= next_run:
            run_sync()
            next_run = _next_run_datetime()
            logging.info(f"Next scheduled sync: {next_run.strftime('%Y-%m-%d %H:%M')}")

        # Sleep 30 seconds between checks (low CPU, still responsive to the minute)
        time.sleep(30)

if __name__ == "__main__":
    main()