import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw
from .utils import add_surrogate_key

def build_dim_products():
    product   = read_raw("product.csv")
    category  = read_raw("product_category.csv")

    dim_products = (
        product.merge(
            category[["category_id", "name", "parent_id"]],
            on="category_id",
            how="left"
        )
        .rename(columns={
            "name_x": "product_name",
            "name_y": "category_name"
        })
        .drop_duplicates(subset=["product_id"])
        .reset_index(drop=True)
    )

    # surrogate key
    dim_products = add_surrogate_key(dim_products, "product_key")

    save_dw(dim_products, "dim_products.csv")
    return dim_products