import os
import pandas as pd
import datetime
from data_cleaning import get_project_root


def get_latest_file_with_prefix(prefix):
    project_root = get_project_root()
    processed_dir = os.path.join(project_root, "data_processed")
    files = [f for f in os.listdir(processed_dir) if f.startswith(prefix)]

    if not files:
        return None

    files = sorted(files, key=lambda x: os.path.getctime(os.path.join(processed_dir, x)))
    return os.path.join(processed_dir, files[-1])


def load_latest_data():
    latest_daily = get_latest_file_with_prefix("kpi_daily_")
    latest_hourly = get_latest_file_with_prefix("kpi_hourly_")
    latest_forecast = get_latest_file_with_prefix("forecast_system_load_")

    if latest_daily is None or latest_hourly is None:
        raise FileNotFoundError("KPI daily/hourly files not found. Run KPI pipeline first.")

    df_daily = pd.read_csv(latest_daily)
    df_hourly = pd.read_csv(latest_hourly)

    df_forecast = None
    if latest_forecast:
        df_forecast = pd.read_csv(latest_forecast)

    return df_daily, df_hourly, df_forecast, latest_daily, latest_hourly, latest_forecast


def evaluate_alerts(df_daily, df_hourly, df_forecast):
    alerts = []

    # --- Daily level alerts (last day) ---
    latest_day = df_daily.iloc[-1]
    date_str = str(latest_day["date"])

    if latest_day["avg_uptime_percent"] < 90:
        alerts.append(
            f"[DAILY] {date_str}: Average uptime below 90% "
            f"({latest_day['avg_uptime_percent']:.1f}%)."
        )

    if latest_day["total_downtime_minutes"] > 60:
        alerts.append(
            f"[DAILY] {date_str}: Total downtime above 60 minutes "
            f"({latest_day['total_downtime_minutes']:.0f} minutes)."
        )

    if latest_day["critical_hours"] > 0:
        alerts.append(
            f"[DAILY] {date_str}: {latest_day['critical_hours']} critical hours detected."
        )

    # --- Hourly level alerts (last 6 hours) ---
    df_hourly["timestamp"] = pd.to_datetime(df_hourly["timestamp"])
    last_6 = df_hourly.sort_values("timestamp").tail(6)

    if last_6["uptime_percent"].mean() < 85:
        alerts.append(
            f"[HOURLY] Last 6 hours: Average uptime below 85% "
            f"({last_6['uptime_percent'].mean():.1f}%)."
        )

    if last_6["errors"].sum() >= 5:
        alerts.append(
            f"[HOURLY] Last 6 hours: High error count "
            f"({last_6['errors'].sum()} errors)."
        )

    # --- Forecast-based alerts (if forecast exists) ---
    if df_forecast is not None:
        max_future_load = df_forecast["predicted_value"].max()
        if max_future_load > 85:
            alerts.append(
                f"[FORECAST] Predicted system load exceeds 85 in the next window "
                f"(max forecast = {max_future_load:.1f})."
            )

    return alerts


def determine_severity(alerts):
    if not alerts:
        return "INFO"

    # Simple severity based on number of issues
    if len(alerts) >= 3:
        return "HIGH"
    elif len(alerts) == 2:
        return "MEDIUM"
    else:
        return "LOW"


def save_alerts(alerts, severity, daily_path, hourly_path, forecast_path):
    project_root = get_project_root()
    logs_dir = os.path.join(project_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    txt_path = os.path.join(logs_dir, f"alerts_{timestamp}.txt")

    with open(txt_path, "w") as f:
        f.write(f"Alert run at: {timestamp}\n")
        f.write(f"Severity: {severity}\n")
        f.write(f"Daily KPI file: {daily_path}\n")
        f.write(f"Hourly KPI file: {hourly_path}\n")
        f.write(f"Forecast file: {forecast_path}\n\n")

        if not alerts:
            f.write("No alerts triggered.\n")
        else:
            f.write("Alerts:\n")
            for a in alerts:
                f.write(f"- {a}\n")

    print("Alert log saved at:", txt_path)


if __name__ == "__main__":
    df_daily, df_hourly, df_forecast, daily_path, hourly_path, forecast_path = load_latest_data()

    alerts = evaluate_alerts(df_daily, df_hourly, df_forecast)
    severity = determine_severity(alerts)

    print("\n--- ALERT SUMMARY ---")
    print("Severity:", severity)
    if not alerts:
        print("No alerts triggered.")
    else:
        for a in alerts:
            print(a)

    save_alerts(alerts, severity, daily_path, hourly_path, forecast_path)
    