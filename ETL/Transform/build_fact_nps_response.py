import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw

def build_fact_nps_response():
    # 1. raw
    nps = read_raw("nps_response.csv", parse_dates=["responded_at"])

    # 2. dimensiones
    cust_dim = pd.read_csv("Data Warehouse/dim_customer.csv")
    chan_dim = pd.read_csv("Data Warehouse/dim_channel.csv")
    time_dim = pd.read_csv("Data Warehouse/dim_time.csv")

    # 3. mapear customer_key
    nps = nps.merge(
        cust_dim[["customer_key", "customer_id"]],
        on="customer_id",
        how="left"
    )

    # 4. mapear channel_key
    nps = nps.merge(
        chan_dim[["channel_key", "channel_id"]],
        on="channel_id",
        how="left"
    )

    # 5. mapear time_key usando responded_at
    nps["date_key"] = nps["responded_at"].dt.strftime("%Y%m%d").astype(int)
    nps = nps.merge(
        time_dim[["time_key", "date_key"]],
        on="date_key",
        how="left"
    )

    # 6. tabla final de hechos NPS
    fact_nps = nps[[
        "nps_id",
        "customer_key",
        "channel_key",
        "time_key",
        "score",
        "comment",
        "responded_at"
    ]].copy()

    # 7. guardar
    save_dw(fact_nps, "fact_nps_response.csv")
    return fact_nps
