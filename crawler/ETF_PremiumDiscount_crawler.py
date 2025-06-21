import urllib.request as req
import requests
import pandas as pd
import datetime
import os

from crawler.worker import app


@app.task()
def crawler_etf_premium_discount():
    print("開始抓取 ETF 折溢價資料...")

    # --- 取得台灣證交所 ETF 清單 ---
    url = "https://www.twse.com.tw/zh/ETFortune/ajaxProductsResult"
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'https://www.twse.com.tw/zh/ETFortune'
    }

    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        records = data.get('data', [])
        df = pd.DataFrame(records)

        # 資料前處理
        df['stockNo'] = df['stockNo'].astype(str).str.zfill(4)
        df = df[['stockNo', 'stockName', 'listingDate', 'indexName', 'totalAv']]
        df.columns = ['股票代號', 'ETF名稱', '上市日期', '標的指數', '資產規模(億元)']

        df['資產規模(億元)'] = df['資產規模(億元)'].astype(str).str.replace(',', '').str.strip()
        df['上市日期'] = pd.to_datetime(df['上市日期'], format='%Y.%m.%d', errors='coerce')
        df['資產規模(億元)'] = pd.to_numeric(df['資產規模(億元)'], errors='coerce')

        # 篩選條件
        df_filtered = df[
            (df['上市日期'].dt.year < 2020) &
            (df['資產規模(億元)'] > 50)
        ][['股票代號']]

        etf_list = df_filtered['股票代號'].tolist()

    except Exception as e:
        print(f"ETF 清單抓取失敗：{e}")
        return

    # --- 抓取每檔 ETF 折溢價資料 ---
    for etf in etf_list:
        try:
            print(f"處理 ETF：{etf}")
            start_date = '2020-01-01'
            end_date = datetime.date.today().strftime('%Y-%m-%d')

            url = f"https://www.moneydj.com/ETF/X/xdjbcd/Basic0003BCD.xdjbcd?etfid={etf}.TW&b={start_date}&c={end_date}"
            response = req.urlopen(url)
            content = response.read().decode('utf-8')

            lines = content.strip().split(' ')
            if len(lines) < 3:
                print(f"資料格式錯誤，跳過 {etf}")
                continue

            dates, navs, prices = lines[0].split(','), lines[1].split(','), lines[2].split(',')
            records = [
                [datetime.datetime.strptime(date, "%Y%m%d"), float(nav), float(price)]
                for date, nav, price in zip(dates, navs, prices)
            ]

            df_etf = pd.DataFrame(records, columns=['交易日期', '淨值', '市價'])
            df_etf['折溢價利率(%)'] = ((df_etf['市價'] - df_etf['淨值']) / df_etf['淨值'] * 100).map(lambda x: f"{x:.2f}%")

            # 儲存為 CSV
            filename = f"MoneyDJ_ETF_PremiumDiscount_{etf}.csv"
            df_etf.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"{etf} 完成，共 {len(df_etf)} 筆，儲存為 {filename}")

        except Exception as e:
            print(f"{etf} 抓取錯誤：{e}")
            continue

    print("ETF 折溢價資料全部抓取完成。")
