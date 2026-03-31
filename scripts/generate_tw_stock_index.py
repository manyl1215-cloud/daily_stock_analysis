import requests
import json
import pandas as pd
import os

def fetch_twse_stocks():
    """獲取台灣上市股票清單"""
    url = "https://isin.twse.com.tw/api/classes.php?req_c=list&r=2"
    headers = {"User-Agent": "Mozilla/5.0"}
    print("Fetching TWSE (上市) stocks...")
    try:
        res = requests.get(url, headers=headers)
        data = res.json()
        stocks = []
        for item in data:
             if '股票' in item.get('type', ''):
                 code = item.get('ticker')
                 name = item.get('name')
                 if code and name and len(code) == 4:
                     stocks.append({
                         "code": code,
                         "name": name,
                         "pinyin": code, # 暫時用代碼代替拼音搜尋
                         "market": "TWSE"
                     })
        return stocks
    except Exception as e:
        print(f"Error fetching TWSE: {e}")
        return []

def fetch_tpex_stocks():
    """獲取台灣上櫃股票清單"""
    url = "https://isin.twse.com.tw/api/classes.php?req_c=list&r=4"
    headers = {"User-Agent": "Mozilla/5.0"}
    print("Fetching TPEx (上櫃) stocks...")
    try:
        res = requests.get(url, headers=headers)
        data = res.json()
        stocks = []
        for item in data:
             if '股票' in item.get('type', ''):
                 code = item.get('ticker')
                 name = item.get('name')
                 if code and name and len(code) == 4:
                     stocks.append({
                         "code": code,
                         "name": name,
                         "pinyin": code,
                         "market": "TPEx"
                     })
        return stocks
    except Exception as e:
        print(f"Error fetching TPEx: {e}")
        return []

if __name__ == "__main__":
    twse_stocks = fetch_twse_stocks()
    tpex_stocks = fetch_tpex_stocks()
    
    all_stocks = twse_stocks + tpex_stocks
    print(f"Total stocks fetched: {len(all_stocks)}")
    
    # 建立與原系統相同的資料結構
    index_data = {
        "metadata": {
            "generated_at": pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "total_stocks": len(all_stocks),
            "market": "TW"
        },
        "stocks": all_stocks
    }
    
    # 存檔至前端 public 目錄
    output_path = os.path.join("apps", "dsa-web", "public", "stocks.index.json")
    
    # 確保目錄存在
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)
        
    print(f"Successfully generated TW stock index at {output_path}")
