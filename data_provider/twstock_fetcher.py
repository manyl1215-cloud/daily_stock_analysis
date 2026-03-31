import yfinance as yf
import pandas as pd
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from data_provider.base import BaseFetcher

logger = logging.getLogger(__name__)

class TWStockFetcher(BaseFetcher):
    """
    專為台股設計的資料獲取器 (基於 yfinance)
    自動處理 .TW (上市) 與 .TWO (上櫃) 的後綴問題
    """
    
    def __init__(self):
        super().__init__()
        self.source_name = "yfinance_tw"
        
    def _format_tw_code(self, code: str) -> str:
        """將純數字代碼轉為 Yahoo Finance 格式"""
        code = str(code).strip()
        # 如果已經有後綴，直接回傳
        if code.endswith('.TW') or code.endswith('.TWO'):
            return code
        
        # 簡單判斷：先假設是上市 (.TW)，如果抓不到再試上櫃 (.TWO)
        # 在實際生產環境中，建議維護一份上市櫃清單對照表以提高效能
        return f"{code}.TW"

    async def get_stock_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """獲取台股歷史日 K 線資料"""
        yf_code = self._format_tw_code(stock_code)
        logger.info(f"Fetching TW stock data for {yf_code} from {start_date} to {end_date}")
        
        try:
            ticker = yf.Ticker(yf_code)
            # yfinance 的 end_date 是不包含的，所以加一天
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            df = ticker.history(start=start_date, end=end_date_obj.strftime("%Y-%m-%d"))
            
            if df.empty and yf_code.endswith('.TW'):
                # 嘗試上櫃代碼
                yf_code = yf_code.replace('.TW', '.TWO')
                logger.info(f"Retrying with OTC code: {yf_code}")
                ticker = yf.Ticker(yf_code)
                df = ticker.history(start=start_date, end=end_date_obj.strftime("%Y-%m-%d"))
                
            if df.empty:
                logger.warning(f"No data found for {stock_code}")
                return pd.DataFrame()
                
            # 轉換為系統標準格式
            df = df.reset_index()
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            df = df.rename(columns={
                'Date': 'trade_date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            
            # 台股成交量通常以「股」為單位，若需要轉為「張」可在此除以 1000
            
            return df[['trade_date', 'open', 'high', 'low', 'close', 'volume']]
            
        except Exception as e:
            logger.error(f"Error fetching TW stock {stock_code}: {e}")
            return pd.DataFrame()

    async def get_realtime_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """獲取即時報價"""
        yf_code = self._format_tw_code(stock_code)
        try:
            ticker = yf.Ticker(yf_code)
            data = ticker.history(period="1d", interval="1m")
            
            if data.empty and yf_code.endswith('.TW'):
                 yf_code = yf_code.replace('.TW', '.TWO')
                 ticker = yf.Ticker(yf_code)
                 data = ticker.history(period="1d", interval="1m")
                 
            if data.empty:
                return None
                
            latest = data.iloc[-1]
            info = ticker.info
            
            prev_close = info.get('previousClose', latest['Close'])
            current_price = latest['Close']
            change_pct = ((current_price - prev_close) / prev_close) * 100 if prev_close else 0
            
            return {
                "stock_code": stock_code,
                "price": current_price,
                "change_pct": change_pct,
                "open": latest['Open'],
                "high": latest['High'],
                "low": latest['Low'],
                "volume": latest['Volume'],
                "update_time": latest.name.strftime("%Y-%m-%d %H:%M:%S")
            }
        except Exception as e:
            logger.error(f"Error fetching realtime data for {stock_code}: {e}")
            return None
