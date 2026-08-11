from __future__ import annotations

import argparse
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from news_fetcher.raw_store import IST, initialize_raw_store
from news_fetcher.db import connect, database_target

ROOT = Path(__file__).resolve().parent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the pipeline once per IST day at 04:00")
    parser.add_argument("--force", action="store_true", help="Run immediately, ignoring the IST time guard")
    parser.add_argument("--database", default=database_target())
    args = parser.parse_args(argv)
    now_ist = datetime.now(IST)
    if not args.force and now_ist.hour != 4:
        print(f"Not due: current IST time is {now_ist:%Y-%m-%d %H:%M}")
        return 0
    run_date = now_ist.date().isoformat()
    with connect(args.database) as connection:
        initialize_raw_store(connection)
        existing = connection.execute(
            "SELECT status FROM pipeline_runs WHERE run_date_ist = ?", (run_date,)
        ).fetchone()
        if existing and existing[0] == "complete" and not args.force:
            print(f"Already complete for {run_date} IST")
            return 0
        connection.execute(
            """INSERT INTO pipeline_runs (run_date_ist, started_at, status, message)
               VALUES (?, ?, 'running', NULL)
               ON CONFLICT(run_date_ist) DO UPDATE SET started_at=excluded.started_at,
                   completed_at=NULL, status='running', message=NULL""",
            (run_date, datetime.now(timezone.utc).isoformat()),
        )
        connection.commit()
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(ROOT / "run_daily.ps1")],
        cwd=ROOT,
    )
    status = "complete" if result.returncode == 0 else "failed"
    with connect(args.database) as connection:
        initialize_raw_store(connection)
        connection.execute(
            "UPDATE pipeline_runs SET completed_at=?, status=?, message=? WHERE run_date_ist=?",
            (datetime.now(timezone.utc).isoformat(), status, f"exit_code={result.returncode}", run_date),
        )
        connection.commit()
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
