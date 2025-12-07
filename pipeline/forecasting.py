import os
import pandas as pd
import numpy as np
import datetime
from sklearn.linear_model import LinearRegression
from data_cleaning import get_project_root

def get_latest_kpi_hourly():
    project_root = get_project_root()
    processed_dir = os.path.join(project_root, "data_processed")
    files = [f for f in os.listdir(processed_dir) if f.startswith("kpi_hourly_")]

    if not files:
        raise FileNotFoundError("No KPI hourly files found in data_processed/")
    
    files = sorted(files, key=lambda x: os.path.getctime(os.path.join(processed_dir, x)))
    return os.path.join(processed_dir, files[-1])

def create_features(df):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Convert timestamp into integer time step for basic forecasting
    df["t"] = range(len(df))

    return df

def build_forecast_model(df, target_col):
    X = df[["t"]].values
    y = df[target_col].values

    model = LinearRegression()
    model.fit(X, y)

    return model

def forecast_next_hours(df, model, hours = 6):
    last_t = df["t"].max()

    future_t = np.array(range(last_t + 1, last_t + 1 + hours)).reshape(-1, 1)
    preds = model.predict(future_t)

    future_df = pd.DataFrame({"t": future_t.flatten(), "predicted_value": preds})

    return future_df

def save_forecast(df, target):
    project_root = get_project_root()
    forecast_dir = os.path.join(project_root, "data_processed")
    os.makedirs(forecast_dir, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(forecast_dir, f"forecast_{target}_{timestamp}.csv")

    df.to_csv(file_path, index=False)
    print(f"Forecast saved for {target} at:", file_path)


def detect_anomalies(df, threshold=20):
    df = df.copy()
    mean_load = df["system_load"].mean()
    std_load = df["system_load"].std()

    upper_bound = mean_load + threshold
    lower_bound = mean_load - threshold

    df["is_anomaly"] = ((df["system_load"] > upper_bound) | (df["system_load"] < lower_bound)).astype(int)
    return df


if __name__ == "__main__":
    # Load latest KPI hourly file
    kpi_hourly_file = get_latest_kpi_hourly()
    print("Using hourly KPI file:", kpi_hourly_file)

    df = pd.read_csv(kpi_hourly_file)

    # Feature preparation
    df_features = create_features(df)

    # Build forecasting model
    model = build_forecast_model(df_features, target_col="system_load")

    # Forecast next 6 hours
    forecast_df = forecast_next_hours(df_features, model, hours=6)

    # Save forecast results
    save_forecast(forecast_df, target="system_load")

    # Detect anomalies in recent data
    df_anomaly = detect_anomalies(df)

    # Show preview
    print("\nForecast Preview:")
    print(forecast_df.head())

    print("\nLatest anomalies:")
    print(df_anomaly.tail())