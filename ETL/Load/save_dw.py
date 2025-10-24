from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]    # subimos desde Load -> ETL -> repo
DW_DIR    = BASE_DIR / "Data Warehouse"

def ensure_dw_dir():
    DW_DIR.mkdir(parents=True, exist_ok=True)

def save_dw(df: pd.DataFrame, filename: str):
    """
    Guarda el DataFrame en la carpeta Data Warehouse/.
    """
    ensure_dw_dir()
    out_path = DW_DIR / filename
    df.to_csv(out_path, index=False)
    print(f"✅ Guardado {filename} en Data Warehouse/")
