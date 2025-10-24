import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw

def build_fact_shipment():
    # 1. Leemos los datos base
    shipment = read_raw("shipment.csv", parse_dates=["shipped_at", "delivered_at"])
    orders = read_raw("sales_order.csv", parse_dates=["order_date"])

    # 2. Leemos dimensiones
    dim_customer = pd.read_csv("Data Warehouse/dim_customer.csv")
    dim_time = pd.read_csv("Data Warehouse/dim_time.csv")

    # 3. Unimos para traer el customer_id desde la orden
    df = shipment.merge(
        orders[["order_id", "customer_id"]],
        on="order_id",
        how="left"
    )

    # 4. Generamos date_key (basado en shipped_at si existe)
    df["date_key"] = pd.NA
    has_shipped = df["shipped_at"].notna()

    df.loc[has_shipped, "date_key"] = (
        df.loc[has_shipped, "shipped_at"].dt.strftime("%Y%m%d")
    )
    df["date_key"] = df["date_key"].astype("Int64")  # permite nulos

    # 5. Mapear time_key (basado en date_key)
    df = df.merge(
        dim_time[["time_key", "date_key"]],
        on="date_key",
        how="left"
    )

    # 6. Mapear customer_key
    df = df.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left"
    )

    # 7. Selección de columnas finales (según DER)
    fact_shipment = df[[
        "shipment_id",      # PK (una fila por envío)
        "order_id",         # FK → ventas
        "customer_key",     # FK → cliente
        "time_key",         # FK → tiempo (fecha de envío)
        "carrier",          # transportista
        "shipped_at",
        "delivered_at"
    ]].copy()

    # 8. Guardamos
    save_dw(fact_shipment, "fact_shipment.csv")
    print("✅ Guardado fact_shipment.csv en Data Warehouse/")

    return fact_shipment
