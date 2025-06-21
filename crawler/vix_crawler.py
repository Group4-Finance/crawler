import yfinance as yf
import pandas as pd
from datetime import datetime
from crawler.worker import app


@app.task()
def crawler_vix_yfinance():
    print("開始下載 VIX 指數資料...")

    try:
        # 初始化 ticker
        vix_ticker = yf.Ticker("^VIX")

        # 取得今日日期字串
        today = datetime.today().strftime('%Y-%m-%d')

        # 取得歷史資料
        vix_data = vix_ticker.history(start="2020-01-01", end=today)
        if vix_data.empty:
            print("VIX 資料為空，可能是網路或 API 問題")
            return

        # 處理欄位格式
        vix_data = vix_data.reset_index()
        vix_data['Date'] = vix_data['Date'].dt.date
        vix_data['Close'] = vix_data['Close'].round(2)
        vix_data = vix_data[['Date', 'Close']]

        # 儲存為 CSV
        filename = "vix_daily.csv"
        vix_data.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"VIX 資料下載完成，總共 {len(vix_data)} 筆，已儲存為 {filename}")

    except Exception as e:
        print(f"下載 VIX 資料失敗：{e}")
