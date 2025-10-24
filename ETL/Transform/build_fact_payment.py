import pandas as pd
from ETL.Extract.read_raw import read_raw
from ETL.Load.save_dw import save_dw

def build_fact_payment():
    # 1. raw
    payment = read_raw("payment.csv", parse_dates=["paid_at"])
    orders = read_raw("sales_order.csv", parse_dates=["order_date"])

    # 2. dimensiones
    dim_payment = pd.read_csv("Data Warehouse/dim_payment.csv")
    dim_customer = pd.read_csv("Data Warehouse/dim_customer.csv")
    dim_time = pd.read_csv("Data Warehouse/dim_time.csv")

    # 3. agregamos customer_id desde la orden
    df = payment.merge(
        orders[["order_id", "customer_id"]],
        on="order_id",
        how="left"
    )

    # 4. traemos payment_key (method+status)
    df = df.merge(
        dim_payment[["payment_key", "method", "status"]],
        on=["method", "status"],
        how="left"
    )

    # 5. traemos customer_key
    df = df.merge(
        dim_customer[["customer_key", "customer_id"]],
        on="customer_id",
        how="left"
    )

    # 6. generamos date_key SOLO donde haya paid_at
    df["date_key"] = pd.NA
    has_paid_at = df["paid_at"].notna()

    df.loc[has_paid_at, "date_key"] = (
        df.loc[has_paid_at, "paid_at"].dt.strftime("%Y%m%d")
    )

    # importantísimo: convertir a Int64 en vez de int nativo
    # Int64 (con I mayúscula) soporta valores nulos
    df["date_key"] = df["date_key"].astype("Int64")

    # 7. mapear time_key desde dim_time
    df = df.merge(
        dim_time[["time_key", "date_key"]],
        on="date_key",
        how="left"
    )

    # 8. seleccionamos las columnas finales
    fact_payment = df[[
        "payment_id",        # grano del hecho
        "order_id",
        "payment_key",       # FK a dim_payment
        "customer_key",      # FK a dim_customer
        "time_key",          # FK a dim_time (puede ser nulo si no se pagó)
        "method",
        "status",
        "amount",
        "paid_at",
        "transaction_ref"
    ]].copy()

    # 9. guardamos
    save_dw(fact_payment, "fact_payment.csv")
    print("✅ Guardado fact_payment.csv en Data Warehouse/")

    return fact_payment
