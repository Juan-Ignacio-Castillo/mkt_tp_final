import sys
from pathlib import Path

# Detecta la ruta raíz del proyecto (esta carpeta)
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

print("Project root:", project_root)

# Imports de las dimensiones
from ETL.Transform.build_dim_products import build_dim_products
from ETL.Transform.build_dim_customer import build_dim_customer
from ETL.Transform.build_dim_store import build_dim_store
from ETL.Transform.build_dim_channel import build_dim_channel
from ETL.Transform.build_dim_time import build_dim_time
from ETL.Transform.build_dim_address import build_dim_address
from ETL.Transform.build_dim_payment_method import build_dim_payment_method
from ETL.Transform.build_fact_order import build_fact_order
from ETL.Transform.build_fact_order_item import build_fact_order_item
from ETL.Transform.build_fact_payment import build_fact_payment
from ETL.Transform.build_fact_shipment import build_fact_shipment
from ETL.Transform.build_fact_nps_response import build_fact_nps_response

def run_pipeline():
    print("🚀 Iniciando pipeline de Data Warehouse...\n")

    print("→ Generando dimensiones...")
    build_dim_products()
    build_dim_store()
    build_dim_channel()
    build_dim_time()
    build_dim_customer()
    build_dim_address()
    build_dim_payment_method()

    print("→ Generando hechos...")
    build_fact_order()
    build_fact_order_item()
    build_fact_payment()
    build_fact_shipment()
    build_fact_nps_response()

    print("\n✅ Pipeline completada. Archivos listos en 'Data Warehouse/'")

if __name__ == "__main__":
    run_pipeline()
