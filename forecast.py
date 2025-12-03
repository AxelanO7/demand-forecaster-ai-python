import pandas as pd
import time
import random
import os
from pytrends.request import TrendReq
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime

# --- CONFIGURATION ---
DISTRICT_FILE = 'District_rows.csv'
SERVICE_FILE = 'ServiceSubCategory_rows.csv'

# Keywords Filter (Hapus distrik non-Bali/Las Vegas)
EXCLUDE_KEYWORDS = [
    'Strip', 'Summerlin', 'Henderson', 'Paradise', 
    'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
]

def load_and_clean_data():
    print("🧹 Loading & Cleaning Data...")
    
    # 1. Load Districts
    if not os.path.exists(DISTRICT_FILE):
        raise FileNotFoundError(f"File {DISTRICT_FILE} tidak ditemukan!")
    
    df_dist = pd.read_csv(DISTRICT_FILE)
    # Filter hanya distrik yang TIDAK mengandung kata-kata blacklist
    df_clean_dist = df_dist[~df_dist['district'].str.contains('|'.join(EXCLUDE_KEYWORDS), case=False, na=False)]
    districts = df_clean_dist['district'].unique().tolist()
    
    # 2. Load Services (Sub Categories)
    if not os.path.exists(SERVICE_FILE):
        raise FileNotFoundError(f"File {SERVICE_FILE} tidak ditemukan!")
        
    df_serv = pd.read_csv(SERVICE_FILE)
    # Ambil kolom 'name', hapus yang kosong, dan bersihkan karakter aneh
    services = df_serv['name'].dropna().unique().tolist()
    # Opsional: Bersihkan nama service (misal "Club Crawl / Bus" jadi "Club Crawl")
    clean_services = [s.split('/')[0].strip() for s in services]
    
    print(f"✅ Loaded {len(districts)} Valid Districts (e.g., {districts[:3]})")
    print(f"✅ Loaded {len(clean_services)} Services (e.g., {clean_services[:3]})")
    
    return districts, clean_services

def get_forecast(series):
    try:
        # Menggunakan Exponential Smoothing
        model = ExponentialSmoothing(series, trend='add', seasonal=None).fit()
        forecast = model.forecast(3) 
        
        next_month_val = forecast.iloc[0]
        three_month_avg = forecast.mean()
        current_val = series.iloc[-1]
        
        if current_val == 0:
            growth = 0
        else:
            growth = ((next_month_val - current_val) / current_val) * 100
            
        return next_month_val, three_month_avg, growth
    except:
        return 0, 0, 0

def main():
    # Setup Google Trends
    pytrends = TrendReq(hl='en-US', tz=360)
    
    # Load Data Dinamis dari CSV
    try:
        districts, services = load_and_clean_data()
    except Exception as e:
        print(f"❌ Error: {e}")
        return

    results = []
    print(f"🚀 Starting Forecast Engine for {len(districts) * len(services)} combinations...")
    print("⚠️  Note: This will take time. Press Ctrl+C to stop early and save partial results.")

    try:
        for district in districts:
            for item in services:
                # Bangun Keyword: "Motorbike Kuta", "Spa Ubud"
                keyword = f"{item} {district}"
                print(f"🔍 Analyzing: {keyword}...", end=" ")
                
                try:
                    # 1. Tarik Data Google Trends
                    pytrends.build_payload([keyword], timeframe='today 12-m', geo='ID')
                    data = pytrends.interest_over_time()
                    
                    if not data.empty:
                        series = data[keyword]
                        
                        # 2. Forecasting
                        pred_next, pred_3m, growth = get_forecast(series)
                        
                        # 3. Logic Status
                        status = "STABLE"
                        action = "Maintain"
                        if growth > 20: 
                            status = "🔥 HIGH DEMAND"
                            action = "Increase Stock"
                        elif growth < -15:
                            status = "❄️ DROPPING"
                            action = "Promo/Discount"
                        
                        print(f"-> Growth: {round(growth,1)}%")
                        
                        results.append({
                            'District': district,
                            'Service Category': item,
                            'Search Keyword': keyword,
                            'Current Index': series.iloc[-1],
                            'Next Month Forecast': round(pred_next, 1),
                            'Growth Trend': f"{round(growth, 1)}%",
                            'Status': status,
                            'Recommendation': action
                        })
                    else:
                        print("-> No Data (Too niche)")
                    
                    # Jeda agar tidak kena limit
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as e:
                    print(f"-> Error: {e}")
                    time.sleep(60) # Jeda lama jika kena limit

    except KeyboardInterrupt:
        print("\n🛑 Process stopped by user. Saving partial results...")

    # Export ke Excel
    if results:
        df_res = pd.DataFrame(results)
        # Urutkan berdasarkan Growth tertinggi agar insight paling penting di atas
        df_res = df_res.sort_values(by='Next Month Forecast', ascending=False)
        
        filename = f"Bali_Demand_Forecast_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        df_res.to_excel(filename, index=False)
        print(f"\n✅ Success! Report saved as: {filename}")
    else:
        print("\n⚠️ No data collected.")

if __name__ == "__main__":
    main()