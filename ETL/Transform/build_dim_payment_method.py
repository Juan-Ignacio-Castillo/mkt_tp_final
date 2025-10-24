import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw
from ETL.Transform.utils import add_surrogate_key

def build_dim_payment_method():

    payment = read_raw("payment.csv")

    dim_payment_method = (
        payment[["method", "status"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    dim_payment_method = add_surrogate_key(dim_payment_method, "payment_key")

    save_dw(dim_payment_method, "dim_payment.csv")
    return dim_payment_method
