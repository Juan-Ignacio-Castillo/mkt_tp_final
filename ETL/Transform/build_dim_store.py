import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw
from .utils import add_surrogate_key

def build_dim_store():
    store = read_raw("store.csv")

    dim_store = store[["store_id", "name"]].rename(columns={
        "name": "store_name"
    })

    dim_store = add_surrogate_key(dim_store, "store_key")

    save_dw(dim_store, "dim_store.csv")
    return dim_store
