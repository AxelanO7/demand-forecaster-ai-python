import pandas as pd
import time
import random
import os
import warnings
from pytrends.request import TrendReq
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime

warnings.filterwarnings("ignore")

# --- KONFIGURASI ---
DISTRICT_FILE = 'District_rows.csv'
SERVICE_FILE = 'ServiceSubCategory_rows.csv'
OUTPUT_FILE = 'Bali_Forecast_Indo_Fixed.xlsx'

# Ambang Batas: Jika rata-rata skor di bawah ini, dianggap "Hantu" (Low Volume)
VOLUME_THRESHOLD = 1.0 
REGION_THRESHOLD = 2

EXCLUDE_KEYWORDS = [
    'Strip', 'Summerlin', 'Henderson', 'Paradise', 
    'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
]

# KAMUS INDONESIA
KEYWORD_MAP = {
    "Motorbike": "Sewa Motor",
    "Car": "Sewa Mobil",
    "Horse Riding": "Berkuda",
    "Nightclub": "Club Malam",
    "Club Crawl": "Party Bali",
    "Fishing": "Mancing",
    "Beauty": "Salon",
    "Tattoo": "Tattoo",
    "Tour": "Paket Wisata",
    "Silver Class": "Silver Class",
    "Diving": "Diving",
    "Surfing": "Surfing",
    "Jeep": "Sewa Jeep",
    "Bottle Service": "Bar Bali",
    "Trekking": "Trekking",
    "Dayclub": "Beach Club",
    "Zoo": "Kebun Binatang",
    "Hair Color": "Salon Rambut",
    "Rafting": "Rafting",
    "Shows": "Tari Bali",
    "Boat": "Tiket Boat",
    "Laundry": "Laundry",
    "ATV": "Main ATV",
    "Spa": "Spa"
}

def load_data():
    if not os.path.exists(DISTRICT_FILE) or not os.path.exists(SERVICE_FILE):
        raise FileNotFoundError("❌ File CSV tidak ditemukan!")
    
    df_dist = pd.read_csv(DISTRICT_FILE)
    df_clean_dist = df_dist[~df_dist['district'].str.contains('|'.join(EXCLUDE_KEYWORDS), case=False, na=False)]
    districts = df_clean_dist['district'].unique().tolist()
    
    df_serv = pd.read_csv(SERVICE_FILE)
    services = df_serv['name'].dropna().unique().tolist()
    clean_services = [s.split('/')[0].strip() for s in services]
    
    return districts, clean_services

def get_forecast_logic(series):
    try:
        # Resample Bulanan
        series_monthly = series.resample('MS').mean()
        
        # Hitung Rata-rata Volume Setahun (Penting!)
        avg_volume = series_monthly.mean()
        
        # Hapus bulan berjalan
        if series_monthly.index[-1].day < 28:
            series_clean = series_monthly[:-1] 
        else:
            series_clean = series_monthly
            
        if len(series_clean) < 3: return 0, 0, 0

        model = ExponentialSmoothing(series_clean, trend='add', seasonal=None).fit()
        forecast = model.forecast(2)
        next_val = forecast.iloc[-1]
        curr_val = series_clean.iloc[-1]
        
        # Logic Growth yang Lebih Jujur
        if curr_val == 0:
            if next_val > 0.1: growth = 100 # Dari 0 jadi ada = Naik 100%
            else: growth = 0 # Tetap mati
        else:
            growth = ((next_val - curr_val) / curr_val) * 100
            
        return next_val, growth, avg_volume
    except:
        return 0, 0, 0

def fetch_safe(pytrends, keywords, context="Data"):
    attempts = 0
    max_retries = 3
    while attempts < max_retries:
        try:
            pytrends.build_payload(keywords, timeframe='today 12-m', geo='ID')
            data = pytrends.interest_over_time()
            return data
        except Exception as e:
            if '429' in str(e):
                wait = 60 + (attempts * 30)
                print(f"\n🛑 429 Limit. Tidur {wait}s...", end="")
                time.sleep(wait)
            else:
                time.sleep(5)
            attempts += 1
            pytrends = TrendReq(hl='en-US', tz=360) 
    return pd.DataFrame()

def main():
    pytrends = TrendReq(hl='en-US', tz=360)
    districts, services = load_data()

    # Smart Resume
    existing_data = []
    processed_keys = set()
    if os.path.exists(OUTPUT_FILE):
        print(f"🔄 Resume: {OUTPUT_FILE}")
        try:
            df_exist = pd.read_excel(OUTPUT_FILE)
            existing_data = df_exist.to_dict('records')
            processed_keys = set([f"{row['Service']} {row['District']}" for row in existing_data])
        except: pass
    else:
        print(f"🆕 File: {OUTPUT_FILE}")

    print(f"🚀 ENGINE STARTED (FIXED LOGIC)")
    print("-" * 60)
    
    try:
        for district in districts:
            # 1. Cek Wilayah
            proxy_kw = f"{district} Bali"
            if f"{services[0]} {district}" in processed_keys: continue 

            print(f"\n🌍 {district.upper()}...", end=" ")
            time.sleep(random.uniform(2, 4))
            region_data = fetch_safe(pytrends, [proxy_kw])
            
            is_dead_region = True
            if not region_data.empty and proxy_kw in region_data.columns:
                score = region_data[proxy_kw].mean()
                if score >= REGION_THRESHOLD:
                    is_dead_region = False
                    print(f"✅ AKTIF (Avg: {score:.1f})")
                else:
                    print(f"❌ SEPI (Avg: {score:.1f}) -> SKIP.")
            else:
                print(f"❌ KOSONG -> SKIP.")

            # 2. Cek Service
            batch_data = []
            for item in services:
                indo_item = KEYWORD_MAP.get(item, item)
                search_kw = f"{indo_item} {district}"
                
                if f"{item} {district}" in processed_keys: continue

                if is_dead_region:
                    batch_data.append({
                        'District': district, 'Service': item, 'Search Keyword': search_kw,
                        'Data Source': "❌ Skipped", 'Forecast Index': 0, 
                        'Growth Forecast %': 0, 'Avg Score': 0,
                        'Status': "➡️ STABLE", 'Action Plan': "Maintain"
                    })
                    print(".", end="")
                else:
                    print(f"\n   🔍 {search_kw:<35}", end=" ")
                    time.sleep(random.uniform(4, 7))
                    data = fetch_safe(pytrends, [search_kw])
                    
                    final_growth = 0
                    avg_vol = 0
                    data_source = "❌ Niche"
                    status = "UNKNOWN"
                    action = "Check"
                    forecast_val = 0

                    if not data.empty and search_kw in data.columns:
                        series = data[search_kw]
                        pred, growth, vol = get_forecast_logic(series)
                        final_growth = growth
                        forecast_val = pred
                        avg_vol = vol
                        
                        if vol < VOLUME_THRESHOLD:
                            data_source = "⚠️ Low Vol"
                            print(f"Low Vol ({vol:.1f})", end="")
                        else:
                            data_source = "✅ Direct"
                            print(f"Growth: {int(growth)}% (Vol: {vol:.1f})", end="")
                    else:
                        print("No Data", end="")
                    
                    # Status Logic Baru
                    if data_source == "✅ Direct":
                        if final_growth > 20: status, action = "🔥 HOT", "Stock Up"
                        elif final_growth < -15: status, action = "❄️ COLD", "Discount"
                        else: status, action = "➡️ STABLE", "Maintain"
                    elif data_source == "⚠️ Low Vol":
                         status, action = "💤 QUIET", "Organic Only"

                    batch_data.append({
                        'District': district, 'Service': item, 'Search Keyword': search_kw,
                        'Data Source': data_source,
                        'Forecast Index': round(forecast_val, 1),
                        'Growth Forecast %': round(final_growth, 1),
                        'Avg Score': round(avg_vol, 1), # Kolom Baru
                        'Status': status, 'Action Plan': action
                    })

            existing_data.extend(batch_data)
            pd.DataFrame(existing_data).to_excel(OUTPUT_FILE, index=False)
            
    except KeyboardInterrupt:
        print("\n🛑 PAUSED.")

    print(f"\n✅ SELESAI. File: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()