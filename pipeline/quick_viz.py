import os
import pandas as pd
import matplotlib.pyplot as plt
from data_cleaning import get_project_root

def get_latest_kpi_daily():
    project_root = get_project_root()
    processed_dir = os.path.join(project_root, "data_processed")
    files = [f for f in os.listdir(processed_dir) if f.startswith("kpi_daily_")]

    if not files:
        raise FileNotFoundError("No KPI daily files found in data_processed/")
    
    files = sorted(files, key=lambda x: os.path.getmtime(os.path.join(processed_dir, x)))
    latest_file = os.path.join(processed_dir, files[-1])
    return latest_file

def main():
    daily_file = get_latest_kpi_daily()
    print("Using KPI daily file:", daily_file)

    df_daily = pd.read_csv(daily_file)

    # Convert date column to datetime
    df_daily['date'] = pd.to_datetime(df_daily['date'])

    # 1) Average Uptime by Month
    plt.figure()
    plt.plot(df_daily['date'], df_daily['avg_uptime_percent'], marker='o')
    plt.xlabel('Date')
    plt.ylabel('Daily Uptime Percent')
    plt.title('Daily Average Uptime by Month')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # 2) Total downtime minutes
    plt.figure()
    plt.bar(df_daily["date"], df_daily["total_downtime_minutes"])
    plt.xlabel("Date")
    plt.ylabel("Total Downtime (minutes)")
    plt.title("Daily Downtime")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # 3) Total errors vs critical hours
    plt.figure()
    plt.plot(df_daily["date"], df_daily["total_errors"], marker="o", label="Total Errors")
    plt.plot(df_daily["date"], df_daily["critical_hours"], marker="s", label="Critical Hours")
    plt.xlabel("Date")
    plt.ylabel("Count")
    plt.title("Daily Errors vs Critical Hours")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()