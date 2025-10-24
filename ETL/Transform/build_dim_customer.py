import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw
from ETL.Transform.utils import add_surrogate_key

def build_dim_customer():
    customer = read_raw("customer.csv")

    dim_customer = customer[[
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "status",
        "created_at"
    ]].drop_duplicates(subset=["customer_id"]).reset_index(drop=True)

    dim_customer = add_surrogate_key(dim_customer, "customer_key")

    save_dw(dim_customer, "dim_customer.csv")
    return dim_customer
