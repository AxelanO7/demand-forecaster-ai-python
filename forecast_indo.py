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
OUTPUT_FILE = 'Bali_Forecast_Indo_Result.xlsx'

REGION_THRESHOLD = 2 # Kita turunkan sedikit thresholdnya karena data Indo volume-nya lebih spesifik

# Filter Wilayah "Sampah"
EXCLUDE_KEYWORDS = [
    'Strip', 'Summerlin', 'Henderson', 'Paradise', 
    'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
]

# --- KAMUS PINTAR (ENGLISH -> INDO QUERY) ---
# Ini memetakan nama service di Database Anda ke Keyword yang dicari orang di Google
KEYWORD_MAP = {
    "Motorbike": "Sewa Motor",
    "Car": "Sewa Mobil",
    "Horse Riding": "Berkuda",
    "Nightclub": "Club Malam", # Atau bisa coba "Clubbing"
    "Club Crawl": "Party Bali", # Club crawl jarang dicari, pakai proxy Party
    "Fishing": "Mancing",
    "Beauty": "Salon",
    "Tattoo": "Tattoo",
    "Tour": "Paket Wisata",
    "Silver Class": "Silver Class",
    "Diving": "Diving",
    "Surfing": "Surfing",
    "Jeep": "Sewa Jeep",
    "Bottle Service": "Bar Bali", # Bottle service terlalu spesifik
    "Trekking": "Trekking",
    "Dayclub": "Beach Club", # Orang lebih sering cari Beach Club daripada Dayclub
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
    # Bersihkan nama service
    clean_services = [s.split('/')[0].strip() for s in services]
    
    return districts, clean_services

def get_forecast_logic(series):
    try:
        series_monthly = series.resample('MS').mean()
        if series_monthly.index[-1].day < 28:
            series_clean = series_monthly[:-1] 
        else:
            series_clean = series_monthly
            
        if len(series_clean) < 3: return 0, 0

        model = ExponentialSmoothing(series_clean, trend='add', seasonal=None).fit()
        forecast = model.forecast(2)
        next_val = forecast.iloc[-1]
        curr_val = series_clean.iloc[-1]
        
        if curr_val == 0: growth = 0
        else: growth = ((next_val - curr_val) / curr_val) * 100
        return next_val, growth
    except:
        return 0, 0

def fetch_safe(pytrends, keywords, context="Data"):
    attempts = 0
    max_retries = 3
    
    while attempts < max_retries:
        try:
            pytrends.build_payload(keywords, timeframe='today 12-m', geo='ID')
            data = pytrends.interest_over_time()
            return data
        except Exception as e:
            msg = str(e)
            if '429' in msg:
                wait = 60 + (attempts * 30)
                print(f"\n🛑 Google Limit (429) di {context}. Tidur {wait}s...")
                time.sleep(wait)
            else:
                print(f"\n⚠️ Error ({msg}). Retry...")
                time.sleep(5)
            attempts += 1
            pytrends = TrendReq(hl='en-US', tz=360) 
    return pd.DataFrame()

def main():
    pytrends = TrendReq(hl='en-US', tz=360)
    
    try:
        districts, services = load_data()
    except Exception as e:
        print(e)
        return

    # --- SMART RESUME ---
    existing_data = []
    processed_keys = set()
    
    if os.path.exists(OUTPUT_FILE):
        print(f"🔄 Resume dari file: {OUTPUT_FILE}")
        try:
            df_exist = pd.read_excel(OUTPUT_FILE)
            existing_data = df_exist.to_dict('records')
            # Resume berdasarkan keyword asli (English) + District
            # Kita buat unique key kombinasi
            processed_keys = set([f"{row['Service']} {row['District']}" for row in existing_data])
        except:
            pass
    else:
        print(f"🆕 File Baru: {OUTPUT_FILE}")

    print(f"🚀 ENGINE STARTED (INDONESIAN KEYWORDS)")
    print(f"ℹ️ Total Antrian: {len(districts)} Wilayah. Mode Skip Aktif.")
    print("-" * 60)
    
    try:
        for district in districts:
            
            # --- 1. CEK WILAYAH ---
            proxy_kw = f"{district} Bali" # Contoh: Kuta Bali
            
            # Cek resume
            unique_key_check = f"{services[0]} {district}"
            if unique_key_check in processed_keys:
                continue 

            print(f"\n🌍 Wilayah: {district.upper()}...", end=" ")
            time.sleep(random.uniform(2, 4))
            
            region_data = fetch_safe(pytrends, [proxy_kw], context="Wilayah")
            is_dead_region = True
            
            if not region_data.empty and proxy_kw in region_data.columns:
                score = region_data[proxy_kw].mean()
                if score >= REGION_THRESHOLD:
                    is_dead_region = False
                    print(f"✅ AKTIF (Score: {score:.1f})")
                else:
                    print(f"❌ SEPI (Score: {score:.1f}) -> SKIP.")
            else:
                print(f"❌ KOSONG -> SKIP.")

            # --- 2. CEK SERVICE ---
            batch_data = [] 
            
            for item in services:
                # 1. Cari terjemahan indo
                indo_item = KEYWORD_MAP.get(item, item) # Default ke asli jika gak ada di kamus
                
                # 2. Keyword Pencarian: "Sewa Motor Kuta"
                search_kw = f"{indo_item} {district}"
                
                # Key untuk resume (tetap pakai nama asli biar konsisten)
                resume_key = f"{item} {district}"
                if resume_key in processed_keys:
                    continue

                if is_dead_region:
                    # Jalur Skip
                    new_row = {
                        'District': district, 'Service': item,
                        'Search Keyword': search_kw, # Kita simpan keyword Indonya
                        'Data Source': "❌ Skipped",
                        'Forecast Index': 0, 'Growth Forecast %': 0,
                        'Status': "➡️ STABLE", 'Action Plan': "Maintain"
                    }
                    batch_data.append(new_row)
                    print(".", end="") 
                    
                else:
                    # Jalur Request
                    print(f"\n   🔍 {search_kw:<35}", end=" ")
                    time.sleep(random.uniform(4, 7)) 
                    
                    data = fetch_safe(pytrends, [search_kw], context="Service")
                    
                    final_growth = 0
                    data_source = "❌ Niche"
                    status = "UNKNOWN"
                    action = "Check"
                    forecast_val = 0

                    if not data.empty and search_kw in data.columns:
                        series = data[search_kw]
                        # Kita turunkan threshold sum jadi > 0 (karena volume indo mungkin kecil tapi ada)
                        if series.sum() > 0:
                            pred, growth = get_forecast_logic(series)
                            final_growth = growth
                            forecast_val = pred
                            data_source = "✅ Direct"
                            print(f"Growth: {int(growth)}%", end="")
                        else:
                            print("Vol: 0", end="")
                    else:
                        print("No Data", end="")
                    
                    # Status Logic
                    if data_source == "✅ Direct":
                        if final_growth > 20: status, action = "🔥 HOT", "Stock Up"
                        elif final_growth < -15: status, action = "❄️ COLD", "Discount"
                        else: status, action = "➡️ STABLE", "Maintain"
                    else:
                        status, action = "➡️ STABLE", "Monitor"

                    new_row = {
                        'District': district, 
                        'Service': item,         # Nama Asli di DB (English)
                        'Search Keyword': search_kw, # Keyword yg dipakai searching (Indo)
                        'Data Source': data_source,
                        'Forecast Index': round(forecast_val, 1),
                        'Growth Forecast %': round(final_growth, 1),
                        'Status': status, 'Action Plan': action
                    }
                    batch_data.append(new_row)
            
            existing_data.extend(batch_data)
            pd.DataFrame(existing_data).to_excel(OUTPUT_FILE, index=False)
            
    except KeyboardInterrupt:
        print("\n\n🛑 PAUSED. Data saved.")

    print(f"\n✅ SELESAI. File: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()