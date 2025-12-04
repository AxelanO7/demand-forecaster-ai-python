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
OUTPUT_FILE = 'Bali_Forecast_Final.xlsx'

# Jika rata-rata skor spesifik di bawah ini, kita pindah ke Proxy Bali
VOLUME_THRESHOLD = 2.0 

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
        series_monthly = series.resample('MS').mean()
        avg_volume = series_monthly.mean()
        
        if series_monthly.index[-1].day < 28:
            series_clean = series_monthly[:-1] 
        else:
            series_clean = series_monthly
            
        if len(series_clean) < 3: return 0, 0, 0

        model = ExponentialSmoothing(series_clean, trend='add', seasonal=None).fit()
        forecast = model.forecast(2)
        next_val = forecast.iloc[-1]
        curr_val = series_clean.iloc[-1]
        
        if curr_val == 0:
            growth = 100 if next_val > 0.1 else 0
        else:
            growth = ((next_val - curr_val) / curr_val) * 100
            
        return next_val, growth, avg_volume
    except:
        return 0, 0, 0

def fetch_safe(pytrends, keywords):
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
                print(f"🛑 429 Limit. Tidur {wait}s...", end="")
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
        print(f"🆕 File Baru: {OUTPUT_FILE}")

    print(f"🚀 ENGINE STARTED (HYBRID PROXY)")
    print("-" * 60)
    
    try:
        for district in districts:
            # 1. Cek Wilayah (Filter Cepat)
            proxy_kw = f"{district} Bali"
            if f"{services[0]} {district}" in processed_keys: continue 

            print(f"\n🌍 {district.upper()}...", end=" ")
            time.sleep(random.uniform(2, 4))
            region_data = fetch_safe(pytrends, [proxy_kw])
            
            is_dead_region = True
            if not region_data.empty and proxy_kw in region_data.columns:
                score = region_data[proxy_kw].mean()
                if score >= 2: # Threshold wilayah
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
                
                # KEYWORD A: SPESIFIK (Misal: "Sewa Motor Kuta")
                specific_kw = f"{indo_item} {district}"
                
                # KEYWORD B: PROXY BALI (Misal: "Sewa Motor Bali")
                # Kita pakai ini kalau Keyword A kosong
                proxy_service_kw = f"{indo_item} Bali" 
                
                if f"{item} {district}" in processed_keys: continue

                if is_dead_region:
                    batch_data.append({
                        'District': district, 'Service': item, 'Search Keyword': specific_kw,
                        'Data Source': "❌ Skipped", 'Forecast Index': 0, 
                        'Growth Forecast %': 0, 'Status': "➡️ STABLE", 'Action Plan': "Maintain"
                    })
                    print(".", end="")
                else:
                    # COBA KEYWORD SPESIFIK DULU
                    print(f"\n   🔍 {specific_kw:<30}", end=" ")
                    time.sleep(random.uniform(3, 5))
                    data = fetch_safe(pytrends, [specific_kw])
                    
                    final_growth = 0
                    forecast_val = 0
                    data_source = "❌ Niche"
                    status = "UNKNOWN"
                    action = "Check"
                    
                    use_proxy = False
                    
                    # Cek Data Spesifik
                    if not data.empty and specific_kw in data.columns:
                        series = data[specific_kw]
                        pred, growth, vol = get_forecast_logic(series)
                        
                        if vol >= VOLUME_THRESHOLD:
                            # DATA BAGUS!
                            final_growth = growth
                            forecast_val = pred
                            data_source = "✅ Direct"
                            print(f"Growth: {int(growth)}% (Vol: {vol:.1f})", end="")
                        else:
                            # DATA JELEK -> GANTI KE PROXY
                            use_proxy = True
                            print(f"Vol Rendah ({vol:.1f}) -> Cek Proxy...", end="")
                    else:
                        use_proxy = True
                        print(f"No Data -> Cek Proxy...", end="")
                        
                    # JIKA PERLU PROXY (Fallback Logic)
                    if use_proxy:
                        time.sleep(random.uniform(3, 5))
                        proxy_data = fetch_safe(pytrends, [proxy_service_kw])
                        
                        if not proxy_data.empty and proxy_service_kw in proxy_data.columns:
                            series = proxy_data[proxy_service_kw]
                            pred, growth, vol = get_forecast_logic(series)
                            
                            final_growth = growth
                            forecast_val = pred # Ini angka Bali, bukan Kuta (hanya referensi trend)
                            data_source = "🔄 Proxy (Bali Trend)"
                            print(f" [PROXY] Growth: {int(growth)}%", end="")
                        else:
                            print(" Proxy juga kosong.", end="")

                    # Status Logic
                    if "✅" in data_source or "🔄" in data_source:
                        if final_growth > 20: status, action = "🔥 HOT", "Stock Up"
                        elif final_growth < -15: status, action = "❄️ COLD", "Discount"
                        else: status, action = "➡️ STABLE", "Maintain"

                    batch_data.append({
                        'District': district, 'Service': item, 
                        'Search Keyword': specific_kw,
                        'Data Source': data_source,
                        'Forecast Index': round(forecast_val, 1),
                        'Growth Forecast %': round(final_growth, 1),
                        'Status': status, 'Action Plan': action
                    })

            existing_data.extend(batch_data)
            pd.DataFrame(existing_data).to_excel(OUTPUT_FILE, index=False)
            
    except KeyboardInterrupt:
        print("\n🛑 PAUSED.")

    print(f"\n✅ SELESAI. File: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()