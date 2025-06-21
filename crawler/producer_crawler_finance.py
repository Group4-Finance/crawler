from crawler.ETF_PremiumDiscount_crawler import crawler_etf_premium_discount

# for 迴圈, 可一次發送多個任務
for stockNo in ["00757", "0052", "00713", "00830", "00733", "00850", "00692", "0050", "00662", "00646"]:
    print(stockNo)
    crawler_etf_premium_discount.delay()