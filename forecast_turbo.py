import pandas as pd
import time
import random
import os
import warnings
from pytrends.request import TrendReq
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime

warnings.filterwarnings("ignore")

# --- KONFIGURASI LOCAL ---
DISTRICT_FILE = 'District_rows.csv'
SERVICE_FILE = 'ServiceSubCategory_rows.csv'
OUTPUT_FILE = 'Bali_Forecast_Turbo_Result.xlsx' # File output baru

# Threshold (0-100). Jika interest wilayah di bawah ini, skip semua service.
# Angka 10 cukup aman untuk memfilter wilayah yang benar-benar sepi.
REGION_THRESHOLD = 5 

# Filter Wilayah Non-Bali (Las Vegas/US)
EXCLUDE_KEYWORDS = [
    'Strip', 'Summerlin', 'Henderson', 'Paradise', 
    'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
]

def load_data():
    """Load data dari CSV lokal"""
    print("📂 Membaca file CSV...")
    if not os.path.exists(DISTRICT_FILE) or not os.path.exists(SERVICE_FILE):
        raise FileNotFoundError("❌ File CSV tidak ditemukan! Pastikan ada di folder yang sama.")
    
    df_dist = pd.read_csv(DISTRICT_FILE)
    df_clean_dist = df_dist[~df_dist['district'].str.contains('|'.join(EXCLUDE_KEYWORDS), case=False, na=False)]
    districts = df_clean_dist['district'].unique().tolist()
    
    df_serv = pd.read_csv(SERVICE_FILE)
    services = df_serv['name'].dropna().unique().tolist()
    clean_services = [s.split('/')[0].strip() for s in services]
    
    return districts, clean_services

def get_forecast_logic(series):
    """Hitung prediksi bulan depan"""
    try:
        series_monthly = series.resample('MS').mean()
        # Hapus bulan berjalan jika belum lengkap
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

def fetch_safe(pytrends, keywords, context="Service"):
    """
    Fungsi Fetch dengan proteksi 429.
    context: Label untuk log (Wilayah atau Service)
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
                wait = 60 + (attempts * 30)
                print(f"\n🛑 Google Limit (429) saat cek {context}. Tidur {wait}s...")
                time.sleep(wait)
            else:
                # Error lain (koneksi putus/timeout)
                print(f"\n⚠️ Error koneksi: {msg}. Retry...")
                time.sleep(5)
            
            attempts += 1
            # Re-init session untuk ganti cookie internal
            pytrends = TrendReq(hl='en-US', tz=360)
            
    return pd.DataFrame()

def main():
    pytrends = TrendReq(hl='en-US', tz=360)
    districts, services = load_data()

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
            print("⚠️ File rusak/kosong. Membuat baru.")
    else:
        print(f"🆕 Membuat file baru: {OUTPUT_FILE}")

    print(f"🚀 TURBO ENGINE STARTED")
    print(f"ℹ️ Total Antrian: {len(districts)} Wilayah x {len(services)} Service")
    print("-" * 60)
    
    try:
        for district in districts:
            
            # --- TAHAP 1: FILTER WILAYAH (The Time Saver) ---
            # Kita cek dulu "Kubu Bali" atau "Sidemen Bali".
            # Jika traffic-nya 0, mustahil ada yang cari "Laundry Kubu".
            
            proxy_kw = f"{district} Bali"
            
            # Cek apakah kita perlu proses wilayah ini?
            # Ambil salah satu sampel service, jika sudah ada di Excel, skip district ini.
            sample_key = f"{services[0]} {district}"
            if sample_key in processed_keys:
                continue 

            print(f"\n🌍 Cek Wilayah: {district.upper()}...", end=" ")
            
            # Sleep agak lama untuk cek wilayah (Penting!)
            time.sleep(random.uniform(4, 7)) 
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

            # --- TAHAP 2: ISI DATA ---
            batch_data = [] # Simpan sementara di memori per wilayah
            
            for item in services:
                target_kw = f"{item} {district}"
                
                # Double check resume
                if target_kw in processed_keys:
                    continue

                if is_dead_region:
                    # JALUR CEPAT (Tanpa Request Google)
                    # Langsung tandai sebagai Niche/Empty
                    new_row = {
                        'District': district,
                        'Service': item,
                        'Search Keyword': target_kw,
                        'Data Source': "❌ Skipped (Quiet Region)",
                        'Forecast Index': 0,
                        'Growth Forecast %': 0,
                        'Status': "➡️ STABLE",
                        'Action Plan': "Maintain"
                    }
                    batch_data.append(new_row)
                    processed_keys.add(target_kw)
                    print(".", end="") # Indikator cepat
                    
                else:
                    # JALUR DETAIL (Request Google)
                    # Hanya dijalankan untuk wilayah ramai (Kuta, Ubud, Canggu, dll)
                    print(f"\n   🔍 {target_kw:<35}", end=" ")
                    
                    # Sleep Wajib (Anti-Blokir)
                    time.sleep(random.uniform(8, 12)) 
                    
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
                    
                    # Tentukan Status
                    if data_source == "✅ Direct":
                        if final_growth > 20: status, action = "🔥 HOT", "Stock Up"
                        elif final_growth < -15: status, action = "❄️ COLD", "Discount"
                        else: status, action = "➡️ STABLE", "Maintain"
                    else:
                        status, action = "➡️ STABLE", "Monitor"

                    new_row = {
                        'District': district,
                        'Service': item,
                        'Search Keyword': target_kw,
                        'Data Source': data_source,
                        'Forecast Index': round(forecast_val, 1),
                        'Growth Forecast %': round(final_growth, 1),
                        'Status': status,
                        'Action Plan': action
                    }
                    batch_data.append(new_row)
                    processed_keys.add(target_kw)
            
            # --- SAVE PER WILAYAH ---
            # Simpan ke Excel setiap 1 wilayah selesai
            existing_data.extend(batch_data)
            pd.DataFrame(existing_data).to_excel(OUTPUT_FILE, index=False)
            
            # Delay ekstra antar wilayah agar lebih aman
            time.sleep(2)

    except KeyboardInterrupt:
        print("\n\n🛑 PAUSED BY USER. Data saved.")

    print(f"\n✅ PROSES SELESAI. File output: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()