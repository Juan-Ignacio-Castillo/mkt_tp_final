import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw
from .utils import add_surrogate_key

def build_dim_time():
    orders = read_raw("sales_order.csv", parse_dates=["order_date"])

    start_date = orders["order_date"].min()
    end_date = orders["order_date"].max()

    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    dim_time = pd.DataFrame({
        "date": dates,
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "month_name": dates.strftime("%B"),
        "week": dates.isocalendar().week,
        "day": dates.day,
        "day_name": dates.strftime("%A"),
        "is_weekend": dates.weekday >= 5
    })

    dim_time = add_surrogate_key(dim_time, "time_key")

    save_dw(dim_time, "dim_time.csv")
    return dim_time
