import pandas as pd
import duckdb
from pathlib import Path
data_dir = Path("data/FBRef_parsed/big5")
db = duckdb.connect()
for f in data_dir.iterdir():
    table_name = f.stem
    print(table_name)
    df = pd.read_csv(f)
    db.register(table_name, df)

