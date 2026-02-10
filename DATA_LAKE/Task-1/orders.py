import pandas as pd
import os

BASE_PATH = 'raw/orders'
os.makedirs(BASE_PATH, exist_ok=True)

data = [
    [101, 1, "2026-02-01", 2500],
    [102, 2, "2026-02-01", 4500],
    [103, 3, "2026-02-01", 3200],
    [104, 1, "2026-02-01", 1800],
    [105, 4, "2026-02-01", 0],        # invalid
    [106, 5, "2026-02-01", -500],     # invalid
    [103, 3, "2026-02-01", 3200]      # duplicate
]

df = pd.DataFrame(
    data,
    columns=["order_id", "user_id", "order_date", "total_amount"]
)

df.to_csv(f"{BASE_PATH}/2026-02-01.csv", index=False)

print("✅ Raw orders CSV created")