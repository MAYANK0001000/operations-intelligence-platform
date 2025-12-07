import os
import pandas as pd
import datetime

from data_cleaning import get_project_root  # reuse helper


def get_latest_clean_file():
    project_root = get_project_root()
    processed_dir = os.path.join(project_root, "data_processed")
    files = [f for f in os.listdir(processed_dir) if f.endswith(".csv")]

    if not files:
        raise FileNotFoundError("No processed CSV files found in data_processed/")

    files = sorted(files, key=lambda x: os.path.getctime(os.path.join(processed_dir, x)))
    latest_file = os.path.join(processed_dir, files[-1])
    return latest_file


def add_kpis(df):
    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Extract date and hour for grouping later
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour

    # Basic KPIs
    df["uptime_minutes"] = 60 - df["downtime_minutes"].clip(lower=0, upper=60)
    df["uptime_percent"] = df["uptime_minutes"] / 60 * 100

    # Error rate per hour (simple proxy)
    df["error_rate"] = df["errors"] / df["uptime_minutes"].replace(0, 1)

    # Flag critical hours
    df["is_critical"] = (
        (df["uptime_percent"] < 80) | (df["errors"] >= 3)
    ).astype(int)

    # Rolling metrics (3-hour window)
    df = df.sort_values("timestamp")
    df["uptime_rolling_mean"] = df["uptime_percent"].rolling(window=3, min_periods=1).mean()
    df["errors_rolling_sum"] = df["errors"].rolling(window=3, min_periods=1).sum()

    return df


def aggregate_daily(df):
    daily = df.groupby("date").agg(
        avg_uptime_percent=("uptime_percent", "mean"),
        total_downtime_minutes=("downtime_minutes", "sum"),
        total_errors=("errors", "sum"),
        critical_hours=("is_critical", "sum")
    ).reset_index()

    return daily


def save_kpi_data(df_hourly, df_daily):
    project_root = get_project_root()
    kpi_dir = os.path.join(project_root, "data_processed")
    os.makedirs(kpi_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    hourly_path = os.path.join(kpi_dir, f"kpi_hourly_{timestamp}.csv")
    daily_path = os.path.join(kpi_dir, f"kpi_daily_{timestamp}.csv")

    df_hourly.to_csv(hourly_path, index=False)
    df_daily.to_csv(daily_path, index=False)

    print("KPI hourly data saved at:", hourly_path)
    print("KPI daily data saved at:", daily_path)


if __name__ == "__main__":
    latest_clean = get_latest_clean_file()
    print("Using cleaned data file:", latest_clean)

    df_clean = pd.read_csv(latest_clean)

    df_kpi = add_kpis(df_clean)
    df_daily = aggregate_daily(df_kpi)

    save_kpi_data(df_kpi, df_daily)

    print("\nHourly KPI preview:")
    print(df_kpi.head())

    print("\nDaily aggregated KPI preview:")
    print(df_daily.head())
