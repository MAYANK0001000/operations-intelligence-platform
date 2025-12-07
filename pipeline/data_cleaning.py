import os
import pandas as pd
import numpy as np
import datetime

# ---------- Helper: locate project folders ----------

def get_project_root():
    # .../pipeline -> .../Operations-intelligence-platform
    pipeline_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.dirname(pipeline_dir)

def get_latest_raw_file():
    project_root = get_project_root()
    raw_dir = os.path.join(project_root, "data_raw")
    files = [f for f in os.listdir(raw_dir) if f.endswith(".csv")]

    if not files:
        raise FileNotFoundError("No raw CSV files found in data_raw/")

    # Pick the latest file by creation time
    files = sorted(files, key=lambda x: os.path.getctime(os.path.join(raw_dir, x)))
    latest_file = os.path.join(raw_dir, files[-1])
    return latest_file

# ---------- Data Quality Checks ----------

def data_quality_checks(df):
    issues = []

    # 1) Missing values
    missing = df.isna().sum()
    for col, count in missing.items():
        if count > 0:
            issues.append(f"Column '{col}' has {count} missing values.")

    # 2) Value ranges
    if (df["system_load"] < 0).any() or (df["system_load"] > 100).any():
        issues.append("Values in 'system_load' are outside expected 0–100 range.")

    if (df["downtime_minutes"] < 0).any():
        issues.append("Negative values found in 'downtime_minutes'.")

    if (df["errors"] < 0).any():
        issues.append("Negative values found in 'errors'.")

    # 3) Duplicated timestamps
    if df["timestamp"].duplicated().any():
        issues.append("Duplicate timestamps detected.")

    return issues

def save_quality_report(issues):
    project_root = get_project_root()
    logs_dir = os.path.join(project_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_path = os.path.join(logs_dir, f"data_quality_report_{timestamp}.txt")

    with open(report_path, "w") as f:
        if not issues:
            f.write("No major data quality issues detected.\n")
        else:
            f.write("Data Quality Issues:\n")
            for issue in issues:
                f.write(f"- {issue}\n")

    print("Data quality report saved at:", report_path)

# ---------- Main cleaning pipeline ----------

def clean_data(df):
    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Sort by time
    df = df.sort_values("timestamp")

    # Example: clip system_load to [0, 100]
    df["system_load"] = df["system_load"].clip(lower=0, upper=100)

    # Replace any negative downtime or errors with 0
    df["downtime_minutes"] = df["downtime_minutes"].clip(lower=0)
    df["errors"] = df["errors"].clip(lower=0)

    # Create a rolling average of system load (smoothing)
    df["system_load_rolling_mean"] = df["system_load"].rolling(window=3, min_periods=1).mean()

    return df

def save_clean_data(df):
    project_root = get_project_root()
    processed_dir = os.path.join(project_root, "data_processed")
    os.makedirs(processed_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(processed_dir, f"clean_data_{timestamp}.csv")
    df.to_csv(file_path, index=False)
    print("Clean data saved at:", file_path)

# ---------- Run everything ----------

if __name__ == "__main__":
    # 1) Load latest raw data
    raw_file_path = get_latest_raw_file()
    print("Using raw data file:", raw_file_path)
    df_raw = pd.read_csv(raw_file_path)

    # 2) Run data quality checks
    issues = data_quality_checks(df_raw)
    save_quality_report(issues)

    # 3) Clean data
    df_clean = clean_data(df_raw)

    # 4) Save processed data
    save_clean_data(df_clean)

    # 5) Show preview
    print(df_clean.head())
