#!/usr/bin/env python3
"""
Binance Mark IV Candlestick Tracker
- Lấy OHLC của MARK IV để vẽ nến volatility
- BACKFILL 90 NGÀY khi startup
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import schedule
import logging
from typing import Optional, List, Dict

# ==================== CẤU HÌNH ====================
TELEGRAM_BOT_TOKEN = "8748933238:AAHXxwTCp39G67Qp68CpMrqzBtuX1jI-sGA"
TELEGRAM_CHAT_ID = "5047088212"
COLLECTION_INTERVAL = 30  # Phút
IV_KLINES_INTERVAL = "30m"  # Interval
SYMBOLS = ["BTC", "ETH"]
EXPIRY_INDEX = 2  # Kỳ hạn thứ 3
DAILY_REPORT_TIME = "23:55"
NUM_ATM_STRIKES = 4
BACKFILL_DAYS = 90  # Số ngày backfill

BINANCE_EAPI_BASE = "https://eapi.binance.com"

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mark_iv_tracker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== GLOBAL ====================
daily_data = pd.DataFrame()
current_expiries = {}


class BinanceMarkIVTracker:
    """Tracker Mark IV với backfill 90 ngày"""
    
    def __init__(self):
        self.base_url = BINANCE_EAPI_BASE
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def get_exchange_info(self) -> Optional[Dict]:
        try:
            url = f"{self.base_url}/eapi/v1/exchangeInfo"
            response = self.session.get(url, timeout=10)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"❌ Lỗi: {e}")
            return None
    
    def get_available_expiries(self, symbol: str) -> List[str]:
        try:
            exchange_info = self.get_exchange_info()
            if not exchange_info:
                return []
            
            expiries = set()
            underlying = f"{symbol}USDT"
            
            for opt in exchange_info.get('optionSymbols', []):
                if opt.get('underlying') == underlying:
                    parts = opt.get('symbol', '').split('-')
                    if len(parts) >= 2:
                        expiries.add(parts[1])
            
            sorted_expiries = sorted(list(expiries))
            logger.info(f"📅 {symbol} - {len(sorted_expiries)} kỳ hạn")
            return sorted_expiries
            
        except Exception as e:
            logger.error(f"❌ Lỗi: {e}")
            return []
    
    def get_dynamic_expiry(self, symbol: str) -> Optional[str]:
        try:
            expiries = self.get_available_expiries(symbol)
            
            if len(expiries) < EXPIRY_INDEX + 1:
                logger.warning(f"⚠️ {symbol} - Không đủ kỳ hạn")
                return expiries[0] if expiries else None
            
            selected_expiry = expiries[EXPIRY_INDEX]
            
            global current_expiries
            if symbol in current_expiries and current_expiries[symbol] != selected_expiry:
                logger.info(f"🔄 {symbol} - Expiry: {current_expiries[symbol]} → {selected_expiry}")
            
            current_expiries[symbol] = selected_expiry
            logger.info(f"📌 {symbol} - Expiry thứ {EXPIRY_INDEX + 1}: {selected_expiry}")
            return selected_expiry
            
        except Exception as e:
            logger.error(f"❌ Lỗi: {e}")
            return None
    
    def get_underlying_price(self, symbol: str) -> Optional[float]:
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT"
            response = self.session.get(url, timeout=10)
            return float(response.json()['price']) if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"❌ Lỗi giá: {e}")
            return None
    
    def get_iv_klines(self, option_symbol: str, interval: str = "30m", limit: int = 500, 
                      start_time: Optional[int] = None, end_time: Optional[int] = None) -> Optional[List]:
        """
        Lấy IV klines (OHLC của Mark IV)
        
        Returns:
            [[timestamp, open_iv, high_iv, low_iv, close_iv, volume, close_time, ...], ...]
        """
        try:
            url = f"{self.base_url}/eapi/v1/ivKlines"
            params = {
                'symbol': option_symbol,
                'interval': interval,
                'limit': limit
            }
            
            if start_time:
                params['startTime'] = start_time
            if end_time:
                params['endTime'] = end_time
            
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.debug(f"Không lấy được IV klines: {option_symbol}")
                return None
                
        except Exception as e:
            logger.debug(f"Lỗi: {e}")
            return None
    
    def get_mark_prices(self) -> Optional[List[Dict]]:
        try:
            url = f"{self.base_url}/eapi/v1/mark"
            response = self.session.get(url, timeout=10)
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            logger.error(f"❌ Lỗi: {e}")
            return None
    
    def filter_atm_strikes(self, all_strikes: List[float], underlying_price: float, num: int = 4) -> List[float]:
        if not all_strikes:
            return []
        
        strikes_dist = [(s, abs(s - underlying_price)) for s in all_strikes]
        strikes_dist.sort(key=lambda x: x[1])
        
        selected = [s for s, _ in strikes_dist[:num]]
        selected.sort()
        
        logger.info(f"   💎 ATM: {[f'${s:,.0f}' for s in selected]}")
        return selected
    
    def backfill_historical_iv_data(self, symbol: str, expiry: str, days: int = 90) -> pd.DataFrame:
        """
        BACKFILL: Lấy historical IV data 90 ngày
        """
        try:
            logger.info(f"🔄 ========== BACKFILL {days} NGÀY - {symbol} ==========")
            
            # Lấy giá underlying hiện tại
            underlying_price = self.get_underlying_price(symbol)
            if not underlying_price:
                return pd.DataFrame()
            
            # Lấy mark prices để có danh sách options
            mark_prices = self.get_mark_prices()
            if not mark_prices:
                return pd.DataFrame()
            
            # Lọc options
            prefix = f"{symbol}-{expiry}-"
            relevant_options = [opt for opt in mark_prices if opt.get('symbol', '').startswith(prefix)]
            
            if not relevant_options:
                logger.warning(f"⚠️ Không có options")
                return pd.DataFrame()
            
            # Lấy strikes
            all_strikes = set()
            for opt in relevant_options:
                parts = opt.get('symbol', '').split('-')
                if len(parts) >= 3:
                    try:
                        all_strikes.add(float(parts[2]))
                    except:
                        continue
            
            all_strikes = sorted(list(all_strikes))
            selected_strikes = self.filter_atm_strikes(all_strikes, underlying_price, NUM_ATM_STRIKES)
            
            if not selected_strikes:
                return pd.DataFrame()
            
            # Tính thời gian
            end_time = int(datetime.now().timestamp() * 1000)
            start_time = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
            
            logger.info(f"   📅 Từ: {datetime.fromtimestamp(start_time/1000)}")
            logger.info(f"   📅 Đến: {datetime.fromtimestamp(end_time/1000)}")
            
            all_data = []
            
            # Lấy data cho từng option
            for opt in relevant_options:
                option_symbol = opt.get('symbol', '')
                parts = option_symbol.split('-')
                
                if len(parts) != 4:
                    continue
                
                strike = float(parts[2])
                if strike not in selected_strikes:
                    continue
                
                option_type = parts[3]
                
                logger.info(f"   📊 Backfill: {option_symbol}")
                
                # Lấy IV klines (có thể cần nhiều requests nếu > 1000 candles)
                current_start = start_time
                option_klines = []
                
                while current_start < end_time:
                    klines = self.get_iv_klines(
                        option_symbol, 
                        IV_KLINES_INTERVAL, 
                        limit=1000,
                        start_time=current_start,
                        end_time=end_time
                    )
                    
                    if not klines:
                        break
                    
                    option_klines.extend(klines)
                    
                    # Update start time cho request tiếp theo
                    if klines:
                        current_start = int(klines[-1][6]) + 1  # close_time + 1
                    else:
                        break
                    
                    time.sleep(0.2)  # Rate limit
                
                logger.info(f"      → {len(option_klines)} candles")
                
                # Parse klines thành rows
                for kline in option_klines:
                    # Format: [timestamp, open_iv, high_iv, low_iv, close_iv, volume, close_time, ...]
                    timestamp = datetime.fromtimestamp(int(kline[0]) / 1000)
                    
                    row = {
                        'Timestamp': timestamp,
                        'Symbol': symbol,
                        'Expiry': expiry,
                        'Strike': strike,
                        'Type': option_type,
                        'Underlying_Price': underlying_price,  # Snapshot hiện tại
                        'Open_IV': float(kline[1]) * 100,  # Convert to %
                        'High_IV': float(kline[2]) * 100,
                        'Low_IV': float(kline[3]) * 100,
                        'Close_IV': float(kline[4]) * 100,
                    }
                    all_data.append(row)
                
                time.sleep(0.5)  # Rate limit giữa options
            
            if all_data:
                df = pd.DataFrame(all_data)
                df = df.sort_values('Timestamp')
                logger.info(f"✅ Backfill: {len(df)} dòng ({days} ngày)")
                return df
            else:
                logger.warning(f"⚠️ Không có backfill data")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ Lỗi backfill: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def collect_current_iv_data(self, symbol: str, expiry: str) -> pd.DataFrame:
        """Thu thập IV data hiện tại (1 snapshot)"""
        try:
            logger.info(f"📊 Thu thập hiện tại: {symbol} - {expiry}")
            
            underlying_price = self.get_underlying_price(symbol)
            if not underlying_price:
                return pd.DataFrame()
            
            mark_prices = self.get_mark_prices()
            if not mark_prices:
                return pd.DataFrame()
            
            prefix = f"{symbol}-{expiry}-"
            relevant_options = [opt for opt in mark_prices if opt.get('symbol', '').startswith(prefix)]
            
            if not relevant_options:
                return pd.DataFrame()
            
            # Lấy strikes
            all_strikes = set()
            for opt in relevant_options:
                parts = opt.get('symbol', '').split('-')
                if len(parts) >= 3:
                    try:
                        all_strikes.add(float(parts[2]))
                    except:
                        continue
            
            selected_strikes = self.filter_atm_strikes(sorted(list(all_strikes)), underlying_price, NUM_ATM_STRIKES)
            
            all_data = []
            
            for opt in relevant_options:
                option_symbol = opt.get('symbol', '')
                parts = option_symbol.split('-')
                
                if len(parts) != 4:
                    continue
                
                strike = float(parts[2])
                if strike not in selected_strikes:
                    continue
                
                option_type = parts[3]
                
                # Lấy IV klines mới nhất
                klines = self.get_iv_klines(option_symbol, IV_KLINES_INTERVAL, limit=1)
                
                if klines and len(klines) > 0:
                    kline = klines[0]
                    timestamp = datetime.fromtimestamp(int(kline[0]) / 1000)
                    
                    row = {
                        'Timestamp': timestamp,
                        'Symbol': symbol,
                        'Expiry': expiry,
                        'Strike': strike,
                        'Type': option_type,
                        'Underlying_Price': underlying_price,
                        'Open_IV': float(kline[1]) * 100,
                        'High_IV': float(kline[2]) * 100,
                        'Low_IV': float(kline[3]) * 100,
                        'Close_IV': float(kline[4]) * 100,
                    }
                    all_data.append(row)
            
            if all_data:
                df = pd.DataFrame(all_data)
                logger.info(f"✅ {symbol}: {len(df)} dòng")
                return df
            else:
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ Lỗi: {e}")
            return pd.DataFrame()
    
    def collect_all_data(self) -> pd.DataFrame:
        """Thu thập data tất cả symbols"""
        all_data = []
        
        for symbol in SYMBOLS:
            try:
                expiry = self.get_dynamic_expiry(symbol)
                if not expiry:
                    continue
                
                df = self.collect_current_iv_data(symbol, expiry)
                if not df.empty:
                    all_data.append(df)
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Lỗi {symbol}: {e}")
                continue
        
        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            logger.info(f"📦 Tổng: {len(combined)} dòng")
            return combined
        else:
            return pd.DataFrame()


class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
    
    def send_message(self, message: str, max_retries=3) -> bool:
        url = f"{self.base_url}/sendMessage"
        params = {'chat_id': self.chat_id, 'text': message, 'parse_mode': 'HTML'}
        
        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=params, timeout=30)
                if response.status_code == 200:
                    logger.info("✅ Telegram OK")
                    return True
            except Exception as e:
                logger.error(f"❌ Telegram ({attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        return False
    
    def send_document(self, file_path: str, caption: str = "", max_retries=3) -> bool:
        url = f"{self.base_url}/sendDocument"
        
        for attempt in range(max_retries):
            try:
                with open(file_path, 'rb') as file:
                    files = {'document': file}
                    data = {'chat_id': self.chat_id, 'caption': caption}
                    response = requests.post(url, data=data, files=files, timeout=60)
                    
                    if response.status_code == 200:
                        logger.info(f"✅ File OK: {file_path}")
                        return True
            except Exception as e:
                logger.error(f"❌ File ({attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        return False


def startup_backfill(tracker: BinanceMarkIVTracker, telegram: TelegramBot):
    """Startup: Backfill 90 ngày + test current data"""
    try:
        logger.info("🚀 ========== STARTUP: BACKFILL 90 NGÀY ==========")
        
        all_backfill_data = []
        
        for symbol in SYMBOLS:
            expiry = tracker.get_dynamic_expiry(symbol)
            if not expiry:
                continue
            
            # BACKFILL 90 ngày
            backfill_df = tracker.backfill_historical_iv_data(symbol, expiry, BACKFILL_DAYS)
            if not backfill_df.empty:
                all_backfill_data.append(backfill_df)
            
            time.sleep(2)
        
        if all_backfill_data:
            combined_backfill = pd.concat(all_backfill_data, ignore_index=True)
            
            # Lưu file backfill
            backfill_filename = f"Mark_IV_Backfill_{BACKFILL_DAYS}d_{datetime.now().strftime('%Y%m%d')}.csv"
            combined_backfill.to_csv(backfill_filename, index=False, encoding='utf-8-sig')
            logger.info(f"💾 Backfill: {backfill_filename}")
            
            # Stats
            total_candles = len(combined_backfill)
            date_range = f"{combined_backfill['Timestamp'].min()} → {combined_backfill['Timestamp'].max()}"
            
            message = (
                f"🚀 <b>Bot Mark IV - BACKFILL HOÀN TẤT!</b>\n\n"
                f"📊 Backfill {BACKFILL_DAYS} ngày:\n"
                f"• Tổng candles: {total_candles:,}\n"
                f"• Khoảng thời gian: {date_range}\n"
                f"• Interval: {IV_KLINES_INTERVAL}\n"
                f"• BTC Expiry: {current_expiries.get('BTC', 'N/A')}\n"
                f"• ETH Expiry: {current_expiries.get('ETH', 'N/A')}\n\n"
                f"✅ Data sẵn sàng vẽ biểu đồ nến IV!"
            )
            
            telegram.send_message(message)
            telegram.send_document(backfill_filename, caption=f"📎 Backfill {BACKFILL_DAYS} ngày - Mark IV OHLC")
            
            logger.info("✅ Backfill hoàn tất!")
            return True
        else:
            message = "⚠️ Backfill thất bại - Không có data"
            telegram.send_message(message)
            return False
        
    except Exception as e:
        logger.error(f"❌ Lỗi backfill: {e}")
        telegram.send_message(f"❌ Backfill fail: {str(e)}")
        return False


def periodic_collection(tracker: BinanceMarkIVTracker):
    global daily_data
    
    try:
        logger.info("⏰ ========== THU THẬP ĐỊNH KỲ ==========")
        
        new_data = tracker.collect_all_data()
        
        if not new_data.empty:
            daily_data = pd.concat([daily_data, new_data], ignore_index=True)
            logger.info(f"📊 Trong ngày: {len(daily_data)} dòng")
        
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")


def daily_report(telegram: TelegramBot):
    global daily_data
    
    try:
        logger.info("📈 ========== BÁO CÁO CUỐI NGÀY ==========")
        
        if daily_data.empty:
            message = "⚠️ Không có data hôm nay"
            telegram.send_message(message)
            return
        
        report_date = datetime.now().strftime('%Y-%m-%d')
        report_filename = f"Mark_IV_Daily_{report_date}.csv"
        
        daily_data.to_csv(report_filename, index=False, encoding='utf-8-sig')
        logger.info(f"💾 Báo cáo: {report_filename}")
        
        stats = (
            f"📈 <b>Báo cáo Mark IV - {report_date}</b>\n\n"
            f"• Tổng: {len(daily_data)} candles\n"
            f"• Symbols: {', '.join(daily_data['Symbol'].unique())}\n"
            f"• Format: OHLC của Mark IV (%)\n"
            f"• Interval: {IV_KLINES_INTERVAL}\n\n"
            f"✅ Sẵn sàng vẽ nến volatility!"
        )
        
        telegram.send_message(stats)
        telegram.send_document(report_filename, caption=f"📎 {report_date}")
        
        daily_data = pd.DataFrame()
        logger.info("🔄 Reset cho ngày mới")
        
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")


def main():
    logger.info("=" * 60)
    logger.info("🤖 MARK IV CANDLESTICK TRACKER")
    logger.info(f"📊 OHLC của Mark IV - Backfill {BACKFILL_DAYS} ngày")
    logger.info("=" * 60)
    
    tracker = BinanceMarkIVTracker()
    telegram = TelegramBot(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    
    # BACKFILL 90 ngày
    if not startup_backfill(tracker, telegram):
        logger.error("❌ Backfill fail!")
        return
    
    logger.info("📅 Lịch...")
    schedule.every(COLLECTION_INTERVAL).minutes.do(periodic_collection, tracker)
    schedule.every().day.at(DAILY_REPORT_TIME).do(daily_report, telegram)
    
    logger.info(f"⏰ Thu thập mỗi {COLLECTION_INTERVAL} phút")
    logger.info(f"📈 Báo cáo {DAILY_REPORT_TIME}")
    logger.info("🏃 Running... (Ctrl+C dừng)")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("\n👋 Tắt...")
        telegram.send_message("🛑 Bot Mark IV đã dừng")
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")
        telegram.send_message(f"🚨 Lỗi: {str(e)}")
    finally:
        logger.info("✅ Tắt")


if __name__ == "__main__":
    main()
