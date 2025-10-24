from pathlib import Path
import pandas as pd

# si necesitás paths, podés importarlos de Extract/Load,
# pero para mantenerlo simple, Transform va a importar read_raw y save_dw,
# entonces acá puede que no necesites paths todavía.

def add_surrogate_key(df: pd.DataFrame, key_name: str):
    """
    Agrega una surrogate key incremental como primera columna.
    """
    df.insert(0, key_name, range(1, len(df) + 1))
    return df
