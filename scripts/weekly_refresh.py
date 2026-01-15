import argparse
from datetime import date
from pathlib import Path

import pandas as pd


# -----------------------------
# Paths / defaults
# -----------------------------
DEFAULT_DATA_PATH = Path("../data/synthetic_students_v1.csv")
DEFAULT_EVENT_LOG_PATH = Path("../data/events/update_events_log.csv")
DEFAULT_SNAPSHOT_DIR = Path("../data/snapshots")
DEFAULT_OUTPUT_DIR = Path("../outputs")


def parse_args():
    p = argparse.ArgumentParser(
        description="Weekly refresh: apply event log updates up to a run date, snapshot data, and regenerate outputs."
    )
    p.add_argument(
        "--run-date",
        type=str,
        default=date.today().isoformat(),
        help="Run date in YYYY-MM-DD format (default: today). Applies all events with event_date <= run-date.",
    )
    p.add_argument(
        "--data-path",
        type=str,
        default=str(DEFAULT_DATA_PATH),
        help="Path to base student CSV.",
    )
    p.add_argument(
        "--event-log-path",
        type=str,
        default=str(DEFAULT_EVENT_LOG_PATH),
        help="Path to append-only event log CSV.",
    )
    p.add_argument(
        "--snapshot-dir",
        type=str,
        default=str(DEFAULT_SNAPSHOT_DIR),
        help="Directory to write dated snapshots.",
    )
    p.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory to write dated outputs.",
    )
    return p.parse_args()


def classify_status(row):
    path = row["career_path"]
    w = int(row["week_in_program"])
    stage = row["milestone_stage"]
    engaged = int(row["observable_engagement"]) == 1

    status = "On Track"

    if path == "Consulting":
        if w >= 9 and stage in ("None", "Applying") and not engaged:
            status = "At Risk"
        elif w >= 7 and stage == "None":
            status = "Behind"

    elif path == "Tech":
        if w >= 13 and stage in ("None", "Applying") and not engaged:
            status = "At Risk"
        elif w >= 11 and stage == "None":
            status = "Behind"

    elif path == "Healthcare":
        if w >= 15 and stage in ("None", "Applying") and not engaged:
            status = "At Risk"
        elif w >= 13 and stage == "None":
            status = "Behind"

    elif path == "Finance":
        if w >= 11 and stage in ("None", "Applying") and not engaged:
            status = "At Risk"
        elif w >= 9 and stage == "None":
            status = "Behind"

    elif path == "Undecided":
        if w >= 9 and stage == "None" and not engaged:
            status = "At Risk"
        elif w >= 7 and stage == "None" and not engaged:
            status = "Behind"

    return status


def recommend_action(row):
    path = row["career_path"]
    status = row["status"]
    stage = row["milestone_stage"]
    engaged = int(row["observable_engagement"]) == 1

    if stage == "Offer":
        return "No action: Celebrate + optional offer evaluation resources"

    if stage == "Interviewing":
        return "Recommend: Interview prep / mock interview resources"

    if status == "On Track":
        if stage == "Applying":
            return f"Recommend: {path} recruiting tips + next relevant workshop"
        return "Recommend: Light-touch resource roundup"

    if status == "Behind":
        if not engaged:
            return f"Send: {path} timeline reminder + top 2 workshops to attend"
        return f"Recommend: Next-step checklist for {path}"

    if not engaged:
        return f"Send: High-urgency nudge + 'start here' resource path for {path}"
    return f"Recommend: Targeted support bundle for {path}"


def main():
    args = parse_args()

    run_date = args.run_date
    data_path = Path(args.data_path)
    event_log_path = Path(args.event_log_path)
    snapshot_dir = Path(args.snapshot_dir)
    output_dir = Path(args.output_dir)

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load base student data
    df = pd.read_csv(data_path)

    required_cols = {
        "student_id",
        "program_type",
        "career_path",
        "week_in_program",
        "milestone_stage",
        "observable_engagement",
    }
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Student data missing required columns: {missing}")

    df["week_in_program"] = df["week_in_program"].astype(int)
    df["observable_engagement"] = df["observable_engagement"].astype(int)

    # Load event log (if missing, treat as empty)
    if event_log_path.exists():
        events = pd.read_csv(event_log_path)
    else:
        events = pd.DataFrame(columns=["student_id", "event_date", "field", "new_value", "source"])

    # Filter events up to run date
    if not events.empty:
        # event_date is YYYY-MM-DD so lexicographic compare works, but we’ll be explicit.
        events["event_date"] = events["event_date"].astype(str)
        events_up_to = events[events["event_date"] <= run_date].copy()
    else:
        events_up_to = events

    # Apply events in chronological order, last-write-wins per (student_id, field)
    updated = df.copy()

    if not events_up_to.empty:
        events_up_to = events_up_to.sort_values(["event_date"])
        for _, ev in events_up_to.iterrows():
            sid = ev["student_id"]
            field = ev["field"]
            new_val = ev["new_value"]

            if field not in updated.columns:
                raise ValueError(f"Invalid event field: {field}")

            # Type coercion for known fields
            if field == "observable_engagement":
                new_val = int(new_val)

            updated.loc[updated["student_id"] == sid, field] = new_val

    # Snapshot “as of run date”
    snapshot_path = snapshot_dir / f"synthetic_students_asof_{run_date}.csv"
    updated.to_csv(snapshot_path, index=False)
    print(f"Snapshot written to: {snapshot_path}")

    # Score
    updated["status"] = updated.apply(classify_status, axis=1)
    updated["recommended_action"] = updated.apply(recommend_action, axis=1)

    # Outputs
    recs_path = output_dir / f"intervention_recommendations_v1_{run_date}.csv"
    updated.sort_values(["career_path", "week_in_program", "student_id"]).to_csv(recs_path, index=False)
    print(f"Recommendations written to: {recs_path}")

    # Bottom-line summary
    summary = (
        updated
        .groupby("career_path")
        .agg(
            total_students=("student_id", "count"),
            pct_on_track=("status", lambda x: (x == "On Track").mean()),
            pct_behind=("status", lambda x: (x == "Behind").mean()),
            pct_at_risk=("status", lambda x: (x == "At Risk").mean()),
            pct_no_observable_engagement=("observable_engagement", lambda x: (x == 0).mean()),
        )
        .reset_index()
    )

    for col in summary.columns:
        if col.startswith("pct_"):
            summary[col] = (summary[col] * 100).round(1)

    bottom_line_path = output_dir / f"career_path_bottom_line_v1_{run_date}.csv"
    summary.to_csv(bottom_line_path, index=False)
    print(f"Bottom-line summary written to: {bottom_line_path}")


if __name__ == "__main__":
    main()
