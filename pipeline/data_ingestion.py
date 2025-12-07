import pandas as pd
import numpy as np
import datetime
import os

# Save raw data to timestamped file
def save_raw_data(df):

    project_root = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(project_root)
    # Ensure directory exists
    save_path = os.path.join(project_root, "data_raw")
    os.makedirs(save_path, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(save_path, f"raw_data_{timestamp}.csv")
    df.to_csv(file_path, index=False)
    print("Raw data saved at : ", file_path)

# Load sample dataset (later replaced with real API)
def load_sample_data():
    data = {
        "timestamp": pd.date_range(start="2023-01-01", periods=50, freq='h'),
        "system_load": np.random.uniform(40, 90, 50),
        "downtime_minutes": np.random.randint(0, 20, 50),
        "errors": np.random.randint(0, 5, 50)
    }
    df = pd.DataFrame(data)
    save_raw_data(df)
    return df

if __name__ == "__main__":
    df = load_sample_data()
    print(df.head())




# NOTE: What this code does:

# ✔ Creates a fake “real-world” dataset
# ✔ Automatically timestamps raw files
# ✔ Mimics what companies do in data pipelines
# ✔ Teaches you real data ingestion concepts


