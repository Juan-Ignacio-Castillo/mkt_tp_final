import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw

def build_dim_address():
    address =  read_raw("address.csv")
    province = read_raw("province.csv")

    dim_address = (
        address
        .merge(
            province[["province_id", "name"]],
            on="province_id",
            how="left"
        )
        .rename(columns={
            "name": "province_name"
        })
    )

    dim_address = dim_address[[
        "address_id",     # surrogate key en la dim (ya existe en raw.address)
        "line1",          # dirección
        "city",
        "province_name",  # viene del merge con province
        "postal_code",
        "country_code"
    ]].drop_duplicates(subset=["address_id"]).reset_index(drop=True)

    save_dw(dim_address, "dim_address.csv")
    return dim_address

