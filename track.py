#!/usr/bin/env python3
"""
Binance European Options Mark IV Tracker Bot
Theo dõi Mark Implied Volatility cho BTC & ETH Options trên Binance EAPI
Chỉ lấy 4 strike prices gần ATM nhất
"""

import requests
import pandas as pd
import time
from datetime import datetime, timedelta
import schedule
import logging
from typing import Optional, List, Dict, Tuple

# ==================== CẤU HÌNH ====================
TELEGRAM_BOT_TOKEN = "8748933238:AAHXxwTCp39G67Qp68CpMrqzBtuX1jI-sGA"
TELEGRAM_CHAT_ID = "5047088212"
COLLECTION_INTERVAL = 30  # Phút
SYMBOLS = ["BTC", "ETH"]  # Cryptocurrency để theo dõi
EXPIRY_INDEX = 2  # Kỳ hạn thứ 3 (index 2)
DAILY_REPORT_TIME = "23:55"  # Thời gian xuất báo cáo hàng ngày
NUM_ATM_STRIKES = 4  # Số strike prices gần ATM (2 phía trên + 2 phía dưới)

# Binance EAPI endpoints
BINANCE_EAPI_BASE = "https://eapi.binance.com"

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('binance_mark_iv_tracker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== GLOBAL VARIABLES ====================
daily_data = pd.DataFrame()  # DataFrame lưu dữ liệu trong ngày
current_expiries = {}  # Lưu kỳ hạn hiện tại cho mỗi symbol


class BinanceEAPITracker:
    """Class quản lý việc theo dõi Mark IV từ Binance EAPI"""
    
    def __init__(self):
        """Khởi tạo tracker"""
        self.base_url = BINANCE_EAPI_BASE
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json'
        })
    
    def get_exchange_info(self) -> Optional[Dict]:
        """
        Lấy thông tin exchange từ Binance EAPI
        
        Returns:
            Dict chứa thông tin exchange hoặc None nếu lỗi
        """
        try:
            url = f"{self.base_url}/eapi/v1/exchangeInfo"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"❌ Lỗi get exchange info: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối Binance EAPI: {e}")
            return None
    
    def get_available_expiries(self, symbol: str) -> List[str]:
        """
        Lấy danh sách các kỳ hạn có sẵn cho symbol
        
        Args:
            symbol: BTC hoặc ETH
            
        Returns:
            List các expiry dates đã được sắp xếp
        """
        try:
            exchange_info = self.get_exchange_info()
            if not exchange_info:
                return []
            
            option_symbols = exchange_info.get('optionSymbols', [])
            expiries = set()
            
            # Lọc theo underlying (BTCUSDT hoặc ETHUSDT)
            underlying = f"{symbol}USDT"
            
            for opt in option_symbols:
                if opt.get('underlying') == underlying:
                    # Symbol format: BTC-260327-100000-C
                    parts = opt.get('symbol', '').split('-')
                    if len(parts) >= 2:
                        expiry = parts[1]  # Ví dụ: 260327
                        expiries.add(expiry)
            
            # Sắp xếp theo thứ tự thời gian
            sorted_expiries = sorted(list(expiries))
            logger.info(f"📅 {symbol} - Tìm thấy {len(sorted_expiries)} kỳ hạn")
            if sorted_expiries:
                logger.info(f"   → {sorted_expiries[:5]}...")
            
            return sorted_expiries
            
        except Exception as e:
            logger.error(f"❌ Lỗi khi lấy expiries cho {symbol}: {e}")
            return []
    
    def get_dynamic_expiry(self, symbol: str) -> Optional[str]:
        """
        Tự động lấy kỳ hạn ở vị trí thứ 3
        
        Args:
            symbol: BTC hoặc ETH
            
        Returns:
            Expiry date hoặc None nếu không tìm thấy
        """
        try:
            expiries = self.get_available_expiries(symbol)
            
            if len(expiries) < EXPIRY_INDEX + 1:
                logger.warning(f"⚠️ {symbol} - Không đủ kỳ hạn (cần ít nhất {EXPIRY_INDEX + 1}, có {len(expiries)})")
                return expiries[0] if expiries else None
            
            selected_expiry = expiries[EXPIRY_INDEX]
            
            # Kiểm tra xem expiry có thay đổi không
            global current_expiries
            if symbol in current_expiries and current_expiries[symbol] != selected_expiry:
                logger.info(f"🔄 {symbol} - Kỳ hạn đã thay đổi: {current_expiries[symbol]} → {selected_expiry}")
            
            current_expiries[symbol] = selected_expiry
            logger.info(f"📌 {symbol} - Sử dụng kỳ hạn thứ {EXPIRY_INDEX + 1}: {selected_expiry}")
            return selected_expiry
            
        except Exception as e:
            logger.error(f"❌ Lỗi khi lấy dynamic expiry cho {symbol}: {e}")
            return None
    
    def get_underlying_price(self, symbol: str) -> Optional[float]:
        """
        Lấy giá hiện tại của underlying asset (spot price)
        
        Args:
            symbol: BTC hoặc ETH
            
        Returns:
            Giá hiện tại hoặc None
        """
        try:
            # Lấy từ Binance Spot API
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}USDT"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                price = float(data['price'])
                logger.debug(f"💰 {symbol} Spot Price: ${price:,.2f}")
                return price
            else:
                logger.error(f"❌ Không lấy được giá {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Lỗi khi lấy giá {symbol}: {e}")
            return None
    
    def get_mark_prices(self) -> Optional[List[Dict]]:
        """
        Lấy mark prices cho tất cả options
        
        Returns:
            List các dict chứa mark price data
        """
        try:
            url = f"{self.base_url}/eapi/v1/mark"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"❌ Lỗi lấy mark prices: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối mark prices API: {e}")
            return None
    
    def filter_atm_strikes(self, all_strikes: List[float], underlying_price: float, num_strikes: int = 4) -> List[float]:
        """
        Lọc lấy các strike prices gần ATM nhất
        
        Args:
            all_strikes: Danh sách tất cả strikes
            underlying_price: Giá underlying hiện tại
            num_strikes: Số strikes cần lấy (mặc định 4)
            
        Returns:
            List các strikes gần ATM nhất
        """
        if not all_strikes:
            return []
        
        # Sắp xếp theo khoảng cách đến underlying price
        strikes_with_distance = [(strike, abs(strike - underlying_price)) for strike in all_strikes]
        strikes_with_distance.sort(key=lambda x: x[1])
        
        # Lấy num_strikes gần nhất
        selected_strikes = [strike for strike, _ in strikes_with_distance[:num_strikes]]
        selected_strikes.sort()  # Sắp xếp lại theo thứ tự tăng dần
        
        logger.info(f"   💎 ATM Strikes (gần ${underlying_price:,.0f}): {[f'${s:,.0f}' for s in selected_strikes]}")
        
        return selected_strikes
    
    def collect_mark_iv_data(self, symbol: str, expiry: str) -> pd.DataFrame:
        """
        Thu thập dữ liệu Mark IV cho một symbol và expiry cụ thể
        CHỈ LẤY 4 STRIKES GẦN ATM NHẤT
        
        Args:
            symbol: BTC hoặc ETH
            expiry: Expiry date (format: 260327)
            
        Returns:
            DataFrame chứa dữ liệu Mark IV
        """
        try:
            logger.info(f"📊 Đang quét {symbol} - Expiry: {expiry}...")
            
            # Lấy giá underlying
            underlying_price = self.get_underlying_price(symbol)
            if underlying_price is None:
                logger.error(f"❌ Không lấy được giá {symbol}")
                return pd.DataFrame()
            
            # Lấy mark prices cho tất cả options
            mark_prices = self.get_mark_prices()
            if not mark_prices:
                logger.error(f"❌ Không lấy được mark prices")
                return pd.DataFrame()
            
            # Lọc theo symbol và expiry
            # Format: BTC-260327-100000-C
            prefix = f"{symbol}-{expiry}-"
            
            relevant_options = [
                opt for opt in mark_prices 
                if opt.get('symbol', '').startswith(prefix)
            ]
            
            if not relevant_options:
                logger.warning(f"⚠️ Không tìm thấy options cho {symbol}-{expiry}")
                return pd.DataFrame()
            
            # Lấy tất cả strikes available
            all_strikes = set()
            for opt in relevant_options:
                parts = opt.get('symbol', '').split('-')
                if len(parts) >= 3:
                    try:
                        strike = float(parts[2])
                        all_strikes.add(strike)
                    except ValueError:
                        continue
            
            all_strikes = sorted(list(all_strikes))
            logger.info(f"   📋 Tổng số strikes: {len(all_strikes)}")
            
            # Lọc lấy chỉ 4 strikes gần ATM nhất
            selected_strikes = self.filter_atm_strikes(all_strikes, underlying_price, NUM_ATM_STRIKES)
            
            if not selected_strikes:
                logger.warning(f"⚠️ Không có strikes nào được chọn")
                return pd.DataFrame()
            
            # Thu thập dữ liệu chỉ cho các strikes đã chọn
            data_rows = []
            current_time = datetime.now()
            
            for opt in relevant_options:
                try:
                    parts = opt.get('symbol', '').split('-')
                    if len(parts) != 4:
                        continue
                    
                    strike_price = float(parts[2])
                    
                    # CHỈ LẤY NẾU STRIKE NẰM TRONG DANH SÁCH ĐÃ CHỌN
                    if strike_price not in selected_strikes:
                        continue
                    
                    option_type = parts[3]  # C (Call) hoặc P (Put)
                    
                    # Lấy Mark IV
                    mark_iv = opt.get('markIV')
                    if mark_iv:
                        mark_iv = float(mark_iv) * 100  # Convert to percentage
                    
                    # Lấy các thông tin khác
                    mark_price = opt.get('markPrice')
                    delta = opt.get('delta')
                    gamma = opt.get('gamma')
                    vega = opt.get('vega')
                    theta = opt.get('theta')
                    
                    # Lưu dữ liệu
                    row = {
                        'Timestamp': current_time,
                        'Symbol': symbol,
                        'Expiry': expiry,
                        'Strike': strike_price,
                        'Type': option_type,
                        'Mark_IV': mark_iv,
                        'Mark_Price': float(mark_price) if mark_price else None,
                        'Delta': float(delta) if delta else None,
                        'Gamma': float(gamma) if gamma else None,
                        'Vega': float(vega) if vega else None,
                        'Theta': float(theta) if theta else None,
                        'Underlying_Price': underlying_price,
                    }
                    data_rows.append(row)
                    
                except Exception as e:
                    logger.debug(f"Bỏ qua option: {e}")
                    continue
            
            # Tạo DataFrame
            df = pd.DataFrame(data_rows)
            
            if not df.empty:
                # Pivot để có Call và Put trong cùng một hàng
                df_pivot = df.pivot_table(
                    index=['Timestamp', 'Symbol', 'Expiry', 'Strike', 'Underlying_Price'],
                    columns='Type',
                    values=['Mark_IV', 'Mark_Price', 'Delta', 'Gamma', 'Vega', 'Theta'],
                    aggfunc='first'
                ).reset_index()
                
                # Flatten column names
                df_pivot.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                                   for col in df_pivot.columns.values]
                
                # Rename columns
                rename_map = {
                    'Mark_IV_C': 'Call_Mark_IV',
                    'Mark_IV_P': 'Put_Mark_IV',
                    'Mark_Price_C': 'Call_Mark_Price',
                    'Mark_Price_P': 'Put_Mark_Price',
                    'Delta_C': 'Call_Delta',
                    'Delta_P': 'Put_Delta',
                    'Gamma_C': 'Call_Gamma',
                    'Gamma_P': 'Put_Gamma',
                    'Vega_C': 'Call_Vega',
                    'Vega_P': 'Put_Vega',
                    'Theta_C': 'Call_Theta',
                    'Theta_P': 'Put_Theta',
                }
                df_pivot.rename(columns=rename_map, inplace=True)
                
                logger.info(f"✅ {symbol} - Đã quét {len(df_pivot)} ATM strikes (từ {len(all_strikes)} tổng strikes)")
                return df_pivot
            else:
                logger.warning(f"⚠️ {symbol} - Không có dữ liệu")
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"❌ Lỗi khi thu thập dữ liệu {symbol}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return pd.DataFrame()
    
    def collect_all_data(self) -> pd.DataFrame:
        """
        Thu thập dữ liệu cho tất cả symbols
        
        Returns:
            DataFrame chứa tất cả dữ liệu
        """
        all_data = []
        
        for symbol in SYMBOLS:
            try:
                # Lấy expiry động
                expiry = self.get_dynamic_expiry(symbol)
                if expiry is None:
                    logger.error(f"❌ Không thể lấy expiry cho {symbol}")
                    continue
                
                # Thu thập dữ liệu
                df = self.collect_mark_iv_data(symbol, expiry)
                if not df.empty:
                    all_data.append(df)
                
                # Nghỉ giữa các requests
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Lỗi khi xử lý {symbol}: {e}")
                continue
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"📦 Tổng cộng: {len(combined_df)} dòng dữ liệu (chỉ ATM strikes)")
            return combined_df
        else:
            logger.warning("⚠️ Không có dữ liệu nào được thu thập")
            return pd.DataFrame()


class TelegramBot:
    """Class quản lý gửi thông báo qua Telegram"""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
    
    def send_message(self, message: str, max_retries=3) -> bool:
        """
        Gửi tin nhắn text
        
        Args:
            message: Nội dung tin nhắn
            max_retries: Số lần thử lại
            
        Returns:
            True nếu thành công, False nếu thất bại
        """
        url = f"{self.base_url}/sendMessage"
        params = {
            'chat_id': self.chat_id,
            'text': message,
            'parse_mode': 'HTML'
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=params, timeout=30)
                if response.status_code == 200:
                    logger.info("✅ Đã gửi tin nhắn Telegram")
                    return True
                else:
                    logger.error(f"❌ Lỗi gửi Telegram: {response.text}")
            except Exception as e:
                logger.error(f"❌ Lỗi kết nối Telegram (Lần {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        
        return False
    
    def send_document(self, file_path: str, caption: str = "", max_retries=3) -> bool:
        """
        Gửi file document
        
        Args:
            file_path: Đường dẫn đến file
            caption: Mô tả file
            max_retries: Số lần thử lại
            
        Returns:
            True nếu thành công, False nếu thất bại
        """
        url = f"{self.base_url}/sendDocument"
        
        for attempt in range(max_retries):
            try:
                with open(file_path, 'rb') as file:
                    files = {'document': file}
                    data = {
                        'chat_id': self.chat_id,
                        'caption': caption
                    }
                    response = requests.post(url, data=data, files=files, timeout=60)
                    
                    if response.status_code == 200:
                        logger.info(f"✅ Đã gửi file: {file_path}")
                        return True
                    else:
                        logger.error(f"❌ Lỗi gửi file: {response.text}")
            except Exception as e:
                logger.error(f"❌ Lỗi gửi file (Lần {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        
        return False


# ==================== CHỨC NĂNG CHÍNH ====================

def startup_test(tracker: BinanceEAPITracker, telegram: TelegramBot):
    """
    Test kích hoạt khi bot vừa khởi động
    """
    try:
        logger.info("🚀 ========== BẮT ĐẦU STARTUP TEST ==========")
        
        # Thu thập dữ liệu test
        test_df = tracker.collect_all_data()
        
        if test_df.empty:
            message = "⚠️ Startup Test: Không thu thập được dữ liệu. Vui lòng kiểm tra kết nối API."
            telegram.send_message(message)
            logger.warning(message)
            return False
        
        # Lưu file test
        test_filename = f"IV_Test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        test_filepath = test_filename  # Lưu trong thư mục hiện tại
        test_df.to_csv(test_filepath, index=False, encoding='utf-8-sig')
        logger.info(f"💾 Đã lưu file test: {test_filepath}")
        
        # Thống kê
        stats_lines = []
        for sym in SYMBOLS:
            sym_data = test_df[test_df['Symbol'] == sym]
            if not sym_data.empty:
                stats_lines.append(f"• {sym}: {len(sym_data)} strikes gần ATM")
        
        # Gửi thông báo và file
        message = (
            "🚀 <b>Bot Mark IV (Binance EAPI) đã kích hoạt thành công!</b>\n"
            f"Đang theo dõi <b>BTC & ETH</b> kỳ hạn thứ {EXPIRY_INDEX + 1}\n"
            f"Chỉ lấy <b>{NUM_ATM_STRIKES} strikes gần ATM nhất</b>\n\n"
            f"📊 Dữ liệu test:\n"
            f"• BTC Expiry: {current_expiries.get('BTC', 'N/A')}\n"
            f"• ETH Expiry: {current_expiries.get('ETH', 'N/A')}\n"
            + '\n'.join(stats_lines) + '\n'
            f"• Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        telegram.send_message(message)
        telegram.send_document(test_filepath, caption="📎 File dữ liệu test (Binance EAPI - ATM strikes)")
        
        logger.info("✅ Startup test hoàn tất!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Lỗi trong startup test: {e}")
        telegram.send_message(f"❌ Startup Test thất bại: {str(e)}")
        return False


def periodic_collection(tracker: BinanceEAPITracker):
    """
    Thu thập dữ liệu định kỳ mỗi 30 phút
    """
    global daily_data
    
    try:
        logger.info("⏰ ========== THU THẬP DỮ LIỆU ĐỊNH KỲ ==========")
        
        # Thu thập dữ liệu
        new_data = tracker.collect_all_data()
        
        if not new_data.empty:
            # Thêm vào daily_data
            daily_data = pd.concat([daily_data, new_data], ignore_index=True)
            logger.info(f"📊 Tổng dữ liệu trong ngày: {len(daily_data)} dòng")
        else:
            logger.warning("⚠️ Không thu thập được dữ liệu mới")
        
    except Exception as e:
        logger.error(f"❌ Lỗi trong periodic collection: {e}")


def daily_report(telegram: TelegramBot):
    """
    Tạo báo cáo cuối ngày và gửi qua Telegram
    """
    global daily_data
    
    try:
        logger.info("📈 ========== TẠO BÁO CÁO CUỐI NGÀY ==========")
        
        if daily_data.empty:
            message = "⚠️ Không có dữ liệu để tạo báo cáo hôm nay."
            telegram.send_message(message)
            logger.warning(message)
            return
        
        # Tạo tên file
        report_date = datetime.now().strftime('%Y-%m-%d')
        report_filename = f"IV_Report_{report_date}.csv"
        
        # Lưu file
        daily_data.to_csv(report_filename, index=False, encoding='utf-8-sig')
        logger.info(f"💾 Đã lưu báo cáo: {report_filename}")
        
        # Thống kê
        stats = (
            f"📈 <b>Báo cáo Mark IV (Binance EAPI) - {report_date}</b>\n\n"
            f"📊 Thống kê:\n"
            f"• Tổng số dòng: {len(daily_data)}\n"
            f"• Số lần thu thập: {daily_data['Timestamp'].nunique()}\n"
            f"• Symbols: {', '.join(daily_data['Symbol'].unique())}\n"
            f"• Chỉ lấy {NUM_ATM_STRIKES} strikes gần ATM/lần\n"
            f"• Thời gian đầu: {daily_data['Timestamp'].min()}\n"
            f"• Thời gian cuối: {daily_data['Timestamp'].max()}\n\n"
            f"✅ Bot sẽ tiếp tục theo dõi cho ngày mới!"
        )
        
        # Gửi báo cáo
        telegram.send_message(stats)
        telegram.send_document(report_filename, caption=f"📎 Báo cáo {report_date}")
        
        # Xóa dữ liệu cũ để bắt đầu ngày mới
        daily_data = pd.DataFrame()
        logger.info("🔄 Đã reset dữ liệu cho ngày mới")
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi tạo báo cáo: {e}")
        telegram.send_message(f"❌ Lỗi tạo báo cáo: {str(e)}")


def main():
    """
    Hàm chính điều khiển bot
    """
    logger.info("=" * 60)
    logger.info("🤖 BINANCE EAPI MARK IV TRACKER BOT")
    logger.info("=" * 60)
    logger.info(f"📌 Chỉ lấy {NUM_ATM_STRIKES} strikes gần ATM nhất")
    
    # Khởi tạo
    tracker = BinanceEAPITracker()
    telegram = TelegramBot(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    
    # Startup test
    startup_success = startup_test(tracker, telegram)
    
    if not startup_success:
        logger.error("❌ Startup test thất bại! Vui lòng kiểm tra cấu hình.")
        return
    
    # Lập lịch
    logger.info("📅 Đang thiết lập lịch trình...")
    
    # Thu thập dữ liệu mỗi 30 phút
    schedule.every(COLLECTION_INTERVAL).minutes.do(periodic_collection, tracker)
    
    # Báo cáo hàng ngày vào 23:55
    schedule.every().day.at(DAILY_REPORT_TIME).do(daily_report, telegram)
    
    logger.info(f"⏰ Lịch trình: Thu thập mỗi {COLLECTION_INTERVAL} phút, Báo cáo lúc {DAILY_REPORT_TIME}")
    logger.info("🏃 Bot đang chạy... (Nhấn Ctrl+C để dừng)")
    
    # Main loop
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Kiểm tra mỗi 1 phút
            
    except KeyboardInterrupt:
        logger.info("\n👋 Đang tắt bot...")
        telegram.send_message("🛑 Bot Mark IV (Binance EAPI) đã dừng hoạt động.")
    except Exception as e:
        logger.error(f"❌ Lỗi nghiêm trọng: {e}")
        telegram.send_message(f"🚨 Bot gặp lỗi nghiêm trọng: {str(e)}")
    finally:
        logger.info("✅ Bot đã tắt.")


if __name__ == "__main__":
    main()
