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
OUTPUT_FILE = 'Bali_Forecast_Biznet_Result.xlsx' # File Output Baru

# Threshold Wilayah (0-100)
# Jika interest wilayah < 5, kita anggap wilayah sepi -> SKIP detailnya.
REGION_THRESHOLD = 5 

# Filter Wilayah "Sampah" (Las Vegas/US)
EXCLUDE_KEYWORDS = [
    'Strip', 'Summerlin', 'Henderson', 'Paradise', 
    'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
]

def load_data():
    if not os.path.exists(DISTRICT_FILE) or not os.path.exists(SERVICE_FILE):
        raise FileNotFoundError("❌ File CSV tidak ditemukan! Pastikan ada di folder.")
    
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
        # Hapus bulan berjalan (incomplete)
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
    """
    Fetch Function Optimized for Business IP.
    Retry lebih sedikit karena koneksi stabil.
    """
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
                # Jika Biznet kena limit, tidurnya agak lama (60s)
                wait = 60 + (attempts * 30)
                print(f"\n🛑 Google Limit (429) di {context}. Tidur {wait}s...")
                time.sleep(wait)
            else:
                print(f"\n⚠️ Error ({msg}). Retry...")
                time.sleep(5)
            
            attempts += 1
            # Reset session (Penting!)
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
        print(f"🔄 Melanjutkan progress dari file: {OUTPUT_FILE}")
        try:
            df_exist = pd.read_excel(OUTPUT_FILE)
            existing_data = df_exist.to_dict('records')
            processed_keys = set(df_exist['Search Keyword'].tolist())
        except:
            pass
    else:
        print(f"🆕 Membuat file baru: {OUTPUT_FILE}")

    print(f"🚀 TURBO ENGINE (BIZNET EDITION) STARTED")
    print(f"ℹ️ Total Antrian: {len(districts)} Wilayah. Mode Skip Aktif.")
    print("-" * 60)
    
    try:
        for district in districts:
            
            # --- TAHAP 1: Cek Popularitas Wilayah ---
            proxy_kw = f"{district} Bali"
            
            # Cek apakah district ini sudah selesai sebelumnya?
            sample_key = f"{services[0]} {district}"
            if sample_key in processed_keys:
                continue 

            print(f"\n🌍 Cek Wilayah: {district.upper()}...", end=" ")
            
            # Sleep Pendek untuk Wilayah (Biznet Kuat)
            time.sleep(random.uniform(2, 4)) 
            region_data = fetch_safe(pytrends, [proxy_kw], context="Wilayah")
            
            is_dead_region = True
            region_score = 0
            
            if not region_data.empty and proxy_kw in region_data.columns:
                region_score = region_data[proxy_kw].mean()
                if region_score >= REGION_THRESHOLD:
                    is_dead_region = False
                    print(f"✅ AKTIF (Score: {region_score:.1f}) -> Masuk Detail")
                else:
                    print(f"❌ SEPI (Score: {region_score:.1f}) -> SKIP {len(services)} Item", end="")
            else:
                print(f"❌ KOSONG -> SKIP {len(services)} Item", end="")

            # --- TAHAP 2: Loop Service ---
            batch_data = [] 
            
            for item in services:
                target_kw = f"{item} {district}"
                
                if target_kw in processed_keys:
                    continue

                if is_dead_region:
                    # JALUR SKIP (Instant)
                    new_row = {
                        'District': district, 'Service': item,
                        'Search Keyword': target_kw,
                        'Data Source': "❌ Skipped (Quiet Region)",
                        'Forecast Index': 0, 'Growth Forecast %': 0,
                        'Status': "➡️ STABLE", 'Action Plan': "Maintain"
                    }
                    batch_data.append(new_row)
                    processed_keys.add(target_kw)
                    print(".", end="") 
                    
                else:
                    # JALUR DETAIL (Request)
                    print(f"\n   🔍 {target_kw:<35}", end=" ")
                    
                    # Sleep Biznet Aman (5-8 Detik)
                    # Tidak perlu 15 detik karena IP Biznet lebih dipercaya
                    time.sleep(random.uniform(5, 8)) 
                    
                    data = fetch_safe(pytrends, [target_kw], context="Service")
                    
                    final_growth = 0
                    data_source = "❌ Niche"
                    status = "UNKNOWN"
                    action = "Check"
                    forecast_val = 0

                    if not data.empty and target_kw in data.columns:
                        series = data[target_kw]
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
                        'District': district, 'Service': item,
                        'Search Keyword': target_kw,
                        'Data Source': data_source,
                        'Forecast Index': round(forecast_val, 1),
                        'Growth Forecast %': round(final_growth, 1),
                        'Status': status, 'Action Plan': action
                    }
                    batch_data.append(new_row)
                    processed_keys.add(target_kw)
            
            # SIMPAN KE EXCEL SETIAP SELESAI 1 WILAYAH
            # Jika crash, data aman.
            existing_data.extend(batch_data)
            pd.DataFrame(existing_data).to_excel(OUTPUT_FILE, index=False)
            
    except KeyboardInterrupt:
        print("\n\n🛑 PAUSED BY USER. Data saved.")

    print(f"\n✅ SELESAI. File output: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()