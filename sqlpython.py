from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    "mssql+pyodbc://sa:123456@localhost\\SQLEXPRESS/FPT_StockDB"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
    fast_executemany=True
)

df = pd.DataFrame([
    {"date":"2025-10-18","open":100.8,"high":101.5,"low":99.9,"close":101.2,"volume":345678}
])
df["date"] = pd.to_datetime(df["date"]).dt.date

df.to_sql("FPT_Stock", con=engine, schema="dbo",
          if_exists="append", index=False, method="multi", chunksize=1000)

print("Đã đẩy dữ liệu vào SQL thành công!")
