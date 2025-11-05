import pandas as pd
from datetime import datetime, timedelta
from vnstock import Vnstock
from sqlalchemy import create_engine

def get_stock_data(symbol="FPT", days=1825):
    """
    Lấy dữ liệu cổ phiếu từ vnstock3 trong khoảng N ngày gần nhất
    """
    end = datetime.today().strftime("%Y-%m-%d")
    start = (datetime.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    fpt = Vnstock().stock(symbol=symbol, source="VCI")
    df = fpt.quote.history(start=start, end=end)
    
    if df.empty:
        print("❌ Không có dữ liệu.")
        return pd.DataFrame()
    
    date_col = "time" if "time" in df.columns else "date"
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.rename(columns={date_col: "date"})
    df = df.sort_values("date")
    return df

def save_to_sql(df):
    """
    Ghi dữ liệu vào SQL Server (tài khoản sa)
    """
    try:
        engine = create_engine(
            "mssql+pyodbc://sa:123456@localhost\\SQLEXPRESS/FPT_StockDB"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes",
            fast_executemany=True
        )

        # Ghi dữ liệu, nếu bảng đã tồn tại thì ghi đè (xóa bảng cũ, tạo bảng mới)
        df.to_sql("FPT_Stock", con=engine, if_exists="replace", index=False)

        print("✅ Đã đẩy dữ liệu vào SQL Server thành công!")
    except Exception as e:
        print("❌ Lỗi khi ghi dữ liệu vào SQL:", e)


def display_stock_analysis(df):
    """
    Hiển thị dashboard phân tích dữ liệu cổ phiếu (KHÔNG VẼ BIỂU ĐỒ)
    """
    print("🚀 PHÂN TÍCH CỔ PHIẾU FPT")
    print("=" * 50)

    if df.empty:
        print("❌ Không có dữ liệu để phân tích.")
        return

    # Tính toán metrics
    current_price = df['close'].iloc[-1]
    price_change = df['close'].iloc[-1] - df['close'].iloc[-2]
    percent_change = (price_change / df['close'].iloc[-2]) * 100
    avg_volume = df['volume'].mean()

    print(f"📊 Giá hiện tại: {current_price:,.0f} VND")
    print(f"📈 Thay đổi: {price_change:+,.0f} VND ({percent_change:+.2f}%)")
    print(f"📦 Khối lượng TB: {avg_volume:,.0f}")
    print(f"📅 Số ngày dữ liệu: {len(df)}")
    print("=" * 50)

    # Bảng dữ liệu
    print("\n📋 DỮ LIỆU CHI TIẾT:")
    df_show = df.copy()
    df_show['date'] = df_show['date'].dt.strftime('%d/%m')
    for col in ['open', 'high', 'low', 'close']:
        df_show[col] = df_show[col].apply(lambda x: f"{x:,.0f}")
    df_show['volume'] = df_show['volume'].apply(lambda x: f"{x:,.0f}")
    print(df_show.to_string(index=False))

# ------------------------- MAIN -------------------------
if __name__ == "__main__":
    # Lấy dữ liệu 5 năm
    df = get_stock_data(symbol="FPT", days=1825)
    
    if not df.empty:
        display_stock_analysis(df)
        save_to_sql(df)
