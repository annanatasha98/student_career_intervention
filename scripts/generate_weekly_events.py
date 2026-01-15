import argparse
import random
from datetime import date
from pathlib import Path

import pandas as pd


# -----------------------------
# Helpers
# -----------------------------
ALLOWED_FIELDS = {"milestone_stage", "observable_engagement"}
ALLOWED_STAGES = ["None", "Applying", "Interviewing", "Offer"]

# Simple, plausible progression (not deterministic; just a demo)
NEXT_STAGE = {
    "None": ["Applying", "None"],              # some stay None
    "Applying": ["Interviewing", "Applying"],  # some progress
    "Interviewing": ["Offer", "Interviewing"], # some progress
    "Offer": ["Offer"],                        # terminal
}

def pick_next_stage(current: str) -> str:
    current = current if current in NEXT_STAGE else "None"
    return random.choice(NEXT_STAGE[current])


def parse_args():
    p = argparse.ArgumentParser(
        description="Generate synthetic weekly update events and append to an event log."
    )
    p.add_argument(
        "--run-date",
        type=str,
        default=date.today().isoformat(),
        help="Run date in YYYY-MM-DD format (default: today).",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42).",
    )
    p.add_argument(
        "--n-events",
        type=int,
        default=6,
        help="How many events to generate (default: 6).",
    )
    p.add_argument(
        "--data-path",
        type=str,
        default="../data/synthetic_students_v1.csv",
        help="Path to the current student CSV.",
    )
    p.add_argument(
        "--event-log-path",
        type=str,
        default="../data/events/update_events_log.csv",
        help="Path to the append-only event log CSV.",
    )
    return p.parse_args()


def main():
    args = parse_args()
    random.seed(args.seed)

    data_path = Path(args.data_path)
    log_path = Path(args.event_log_path)
    run_date = args.run_date

    students = pd.read_csv(data_path)

    required_cols = {
        "student_id",
        "program_type",
        "career_path",
        "week_in_program",
        "milestone_stage",
        "observable_engagement",
    }
    missing = required_cols - set(students.columns)
    if missing:
        raise ValueError(f"Student data missing required columns: {missing}")

    # Candidate pool: focus updates on students who could plausibly change
    # (e.g., not already Offer for milestone updates)
    candidate_ids = students["student_id"].tolist()
    if not candidate_ids:
        raise ValueError("No students found in the dataset.")

    events = []

    # Generate a mix of engagement flips + milestone progressions
    # Not "real-time": this represents weekly batch updates.
    for _ in range(args.n_events):
        sid = random.choice(candidate_ids)

        # Randomly choose to generate an engagement event or milestone event
        if random.random() < 0.45:
            # observable_engagement: 0 -> 1 more common than 1 -> 0
            current = int(students.loc[students["student_id"] == sid, "observable_engagement"].iloc[0])
            if current == 0:
                new_val = 1
            else:
                # Rarely lose visibility in a demo
                new_val = current if random.random() < 0.85 else 0

            events.append(
                {
                    "student_id": sid,
                    "event_date": run_date,
                    "field": "observable_engagement",
                    "new_value": str(new_val),
                    "source": "synthetic_weekly_generator",
                }
            )
        else:
            # milestone_stage: progress forward sometimes, sometimes stays
            current_stage = str(students.loc[students["student_id"] == sid, "milestone_stage"].iloc[0])
            new_stage = pick_next_stage(current_stage)

            # Avoid generating nonsense regressions like Offer -> Applying
            # pick_next_stage already prevents that.
            events.append(
                {
                    "student_id": sid,
                    "event_date": run_date,
                    "field": "milestone_stage",
                    "new_value": new_stage,
                    "source": "synthetic_weekly_generator",
                }
            )

    events_df = pd.DataFrame(events)

    # Ensure log exists with header
    log_path.parent.mkdir(parents=True, exist_ok=True)
    if not log_path.exists():
        log_path.write_text("student_id,event_date,field,new_value,source\n", encoding="utf-8")

    # Append
    existing = pd.read_csv(log_path)
    combined = pd.concat([existing, events_df], ignore_index=True)

    # Basic cleanup: drop exact duplicates (same student, same date, same field, same new_value)
    combined = combined.drop_duplicates(subset=["student_id", "event_date", "field", "new_value"])

    # Validate fields
    bad_fields = set(combined["field"]) - ALLOWED_FIELDS
    if bad_fields:
        raise ValueError(f"Event log contains invalid fields: {bad_fields}")

    combined.to_csv(log_path, index=False)
    print(f"Appended {len(events_df)} generated events to: {log_path}")
    print("Sample of newly generated events:")
    print(events_df.to_string(index=False))


if __name__ == "__main__":
    main()