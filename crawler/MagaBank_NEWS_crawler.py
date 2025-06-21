import requests
import bs4 as bs
import pandas as pd
from datetime import datetime, timedelta
import jieba
import jieba.analyse
import logging

from crawler.worker import app

# 關閉 jieba 記錄器
jieba.setLogLevel(logging.WARNING)

@app.task()
def crawler_megabank_news(start="2020-01-01"):
    print("開始抓取兆豐基金新聞資料...")

    h = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        start_date = datetime.strptime(start, "%Y-%m-%d").date()
        end_date = datetime.today().date()
    except Exception as e:
        print(f"日期格式錯誤：{e}")
        return

    table = []
    current_date = start_date

    while current_date <= end_date:
        date_str = f"{current_date.year}-{current_date.month}-{current_date.day}"

        for page in range(1, 6):
            url = f"https://fund.megabank.com.tw/w/wp/wu01megaNews.djhtm?A=NA&B={date_str}&C=NA&Page={page}"
            try:
                response = requests.get(url, headers=h, timeout=10)
                if response.status_code != 200:
                    print(f"跳過頁面 {url}，HTTP 錯誤：{response.status_code}")
                    continue

                html = bs.BeautifulSoup(response.text, features="html.parser")
                for tr in html.find_all('tr'):
                    en_date = tr.find('td', class_=['wfb2c', 'wfb5c'])
                    en_title = tr.find('a')

                    if en_date and en_title:
                        try:
                            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                            formatted_date = date_obj.strftime("%Y/%m/%d")

                            if en_date.text.strip() == formatted_date:
                                date_final = en_date.text.strip()
                                title_final = en_title.text.strip()
                                url_final = "https://fund.megabank.com.tw" + en_title['href']
                                tags = jieba.analyse.extract_tags(title_final)

                                data = {
                                    "日期": date_final,
                                    "標題": title_final,
                                    "連結": url_final,
                                    "標籤": tags
                                }

                                if data not in table:
                                    table.append(data)

                        except Exception as e:
                            print(f"單筆解析錯誤：{e}")
                            continue

            except Exception as e:
                print(f"請求失敗：{url}，錯誤：{e}")
                continue

        current_date += timedelta(days=1)

    # 儲存結果
    if table:
        df = pd.json_normalize(table)
        df.to_csv("megabank_news.csv", index=False, encoding="utf-8-sig")
        print(f"新聞資料抓取完成，共 {len(df)} 筆，已儲存為 megabank_news.csv")
    else:
        print("沒有可用資料，未產生 CSV。")
