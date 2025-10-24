import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw

def build_fact_order():
    # 1. Cargar datos crudos
    orders = read_raw("sales_order.csv", parse_dates=["order_date"])

    # 2. Cargar dimensiones
    dim_customer = pd.read_csv("Data Warehouse/dim_customer.csv")
    dim_store = pd.read_csv("Data Warehouse/dim_store.csv")
    dim_channel = pd.read_csv("Data Warehouse/dim_channel.csv")
    dim_time = pd.read_csv("Data Warehouse/dim_time.csv")

    # 3. Mapear claves surrogate
    # time_key (por order_date)
    orders["date_key"] = orders["order_date"].dt.strftime("%Y%m%d").astype(int)
    orders = orders.merge(
        dim_time[["time_key", "date_key"]],
        on="date_key",
        how="left"
    )

    # customer_key
    orders = orders.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left"
    )

    # store_key
    orders = orders.merge(
        dim_store[["store_key", "store_id"]],
        on="store_id",
        how="left"
    )

    # channel_key
    orders = orders.merge(
        dim_channel[["channel_key", "channel_id"]],
        on="channel_id",
        how="left"
    )

    # 4. Seleccionar columnas finales
    fact_order = orders[[
        "order_id",
        "time_key",
        "customer_key",
        "store_key",
        "channel_key",
        "subtotal",        # nombre real según DER
        "shipping_fee",
        "tax_amount",
        "total_amount"
    ]].copy()

    # 5. Guardar en Data Warehouse
    save_dw(fact_order, "fact_order.csv")
    print("✅ Guardado fact_order.csv en Data Warehouse/")
    return fact_order
