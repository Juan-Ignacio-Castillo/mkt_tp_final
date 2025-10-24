import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw
from .utils import add_surrogate_key

def build_dim_channel():
    channel = read_raw("channel.csv")

    dim_channel = channel[["channel_id", "code", "name"]].rename(columns={
        "name": "channel_name",
        "code": "channel_code"
    })

    dim_channel = add_surrogate_key(dim_channel, "channel_key")

    save_dw(dim_channel, "dim_channel.csv")
    return dim_channel