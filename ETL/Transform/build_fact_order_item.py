import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw

def build_fact_order_item():
    # 1. Leemos tablas raw
    items = read_raw("sales_order_item.csv")
    orders = read_raw("sales_order.csv", parse_dates=["order_date"])

    # 2. Leemos dimensiones necesarias
    dim_product = pd.read_csv("Data Warehouse/dim_products.csv")
    dim_customer = pd.read_csv("Data Warehouse/dim_customer.csv")
    dim_store = pd.read_csv("Data Warehouse/dim_store.csv")
    dim_channel = pd.read_csv("Data Warehouse/dim_channel.csv")
    dim_time = pd.read_csv("Data Warehouse/dim_time.csv")

    # 3. Sumamos contexto de la orden a cada ítem:
    #    - para saber cliente, canal, tienda, y fecha
    items_merged = items.merge(
        orders[[
            "order_id",
            "customer_id",
            "store_id",
            "channel_id",
            "order_date"
        ]],
        on="order_id",
        how="left"
    )

    # 4. time_key (por fecha de la orden)
    items_merged["date_key"] = items_merged["order_date"].dt.strftime("%Y%m%d").astype(int)
    items_merged = items_merged.merge(
        dim_time[["time_key", "date_key"]],
        on="date_key",
        how="left"
    )

    # 5. product_key
    items_merged = items_merged.merge(
        dim_product[["product_key", "product_id"]],
        on="product_id",
        how="left"
    )

    # 6. customer_key
    items_merged = items_merged.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left"
    )

    # 7. store_key
    items_merged = items_merged.merge(
        dim_store[["store_key", "store_id"]],
        on="store_id",
        how="left"
    )

    # 8. channel_key
    items_merged = items_merged.merge(
        dim_channel[["channel_key", "channel_id"]],
        on="channel_id",
        how="left"
    )

    # 9. Selección de columnas finales de la fact
    # Según tu DER:
    #  - quantity (no qty)
    #  - unit_price
    #  - discount_amount
    #  - line_total
    fact_order_item = items_merged[[
        "order_item_id",     # PK grano linea
        "order_id",
        "time_key",
        "product_key",
        "customer_key",
        "store_key",
        "channel_key",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_total"
    ]].copy()

    # 10. Guardar resultado
    save_dw(fact_order_item, "fact_order_item.csv")
    print("✅ Guardado fact_order_item.csv en Data Warehouse/")

    return fact_order_item
