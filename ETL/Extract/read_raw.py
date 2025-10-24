from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]  # subimos desde Extract -> ETL -> repo
RAW_DIR = BASE_DIR / "raw"

def read_raw(csv_name: str, **kwargs) -> pd.DataFrame:
    """
    Devuelve un DataFrame leyendo un archivo de raw/.
    """
    return pd.read_csv(RAW_DIR / csv_name, **kwargs)
