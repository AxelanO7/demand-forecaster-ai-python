import pandas as pd
import time
import random
import os
import warnings
from pytrends.request import TrendReq
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime

# Suppress warnings
warnings.filterwarnings("ignore")

class BaliDemandForecaster:
    def __init__(self):
        # --- CONFIGURATION ---
        self.BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.DISTRICT_FILE = os.path.join(self.BASE_DIR, 'data', 'raw', 'District_rows.csv')
        self.SERVICE_FILE = os.path.join(self.BASE_DIR, 'data', 'raw', 'ServiceSubCategory_rows.csv')
        self.OUTPUT_FILE = os.path.join(self.BASE_DIR, 'data', 'output', 'Bali_Forecast_Report_EN.xlsx')
        
        # Thresholds
        self.VOLUME_THRESHOLD = 2.0  # If specific volume < 2.0, switch to proxy
        self.REGION_THRESHOLD = 5.0  # If region score < 5.0, skip entire region
        
        # Exclusions
        self.EXCLUDE_KEYWORDS = [
            'Strip', 'Summerlin', 'Henderson', 'Paradise', 
            'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
        ]

        # --- SMART MAPPING ---
        # Key (English) -> Value (Indonesian Search Query)
        # We search in Indo (Accurate), Report in English (Professional)
        self.KEYWORD_MAP = {
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
        
        # Initialize Google Trends
        self.pytrends = TrendReq(hl='en-US', tz=360)

    def load_data(self):
        if not os.path.exists(self.DISTRICT_FILE) or not os.path.exists(self.SERVICE_FILE):
            raise FileNotFoundError("❌ Raw data not found in data/raw/")
        
        print("📂 Loading Data...")
        df_dist = pd.read_csv(self.DISTRICT_FILE)
        df_clean_dist = df_dist[~df_dist['district'].str.contains('|'.join(self.EXCLUDE_KEYWORDS), case=False, na=False)]
        districts = df_clean_dist['district'].unique().tolist()
        
        df_serv = pd.read_csv(self.SERVICE_FILE)
        services = df_serv['name'].dropna().unique().tolist()
        clean_services = [s.split('/')[0].strip() for s in services]
        
        return districts, clean_services

    def fetch_safe(self, keywords, context="Data"):
        """
        Fetches data with AGGRESSIVE BACKOFF to prevent 429.
        """
        attempts = 0
        max_retries = 3
        while attempts < max_retries:
            try:
                self.pytrends.build_payload(keywords, timeframe='today 12-m', geo='ID')
                data = self.pytrends.interest_over_time()
                return data
            except Exception as e:
                # If 429, sleep exponentially (60s -> 120s -> 180s)
                if '429' in str(e):
                    wait = 60 + (attempts * 60) 
                    print(f"\n🛑 Rate Limit (429) at {context}. Sleeping {wait}s...", end="")
                    time.sleep(wait)
                else:
                    # Other errors
                    time.sleep(10)
                
                attempts += 1
                self.pytrends = TrendReq(hl='en-US', tz=360) 
        
        return pd.DataFrame()

    def calculate_forecast(self, series):
        try:
            series_monthly = series.resample('MS').mean()
            avg_vol = series_monthly.mean()
            
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
                
            return next_val, growth, avg_vol
        except:
            return 0, 0, 0

    def run(self):
        districts, services = self.load_data()
        
        # Smart Resume
        existing_data = []
        processed_keys = set()
        if os.path.exists(self.OUTPUT_FILE):
            print(f"🔄 Resuming from: {self.OUTPUT_FILE}")
            try:
                df_exist = pd.read_excel(self.OUTPUT_FILE)
                existing_data = df_exist.to_dict('records')
                # Key based on English Service Name + District
                processed_keys = set([f"{row['Service Category']} {row['District']}" for row in existing_data])
            except: pass
        else:
            print(f"🆕 Creating new report: {self.OUTPUT_FILE}")

        print(f"🚀 ENGINE STARTED (Ultra-Safe Mode) | Queue: {len(districts)} Districts")
        print("-" * 60)

        try:
            for district in districts:
                # 1. Region Check
                proxy_kw = f"{district} Bali"
                if f"{services[0]} {district}" in processed_keys: continue 

                print(f"\n🌍 {district.upper()}...", end=" ")
                
                # Jeda Awal Wilayah (5-8 detik)
                time.sleep(random.uniform(5, 8))
                
                region_data = self.fetch_safe([proxy_kw], context="Region")
                
                is_dead_region = True
                if not region_data.empty and proxy_kw in region_data.columns:
                    score = region_data[proxy_kw].mean()
                    if score >= self.REGION_THRESHOLD:
                        is_dead_region = False
                        print(f"✅ ACTIVE (Avg: {score:.1f})")
                    else:
                        print(f"❌ QUIET (Avg: {score:.1f}) -> SKIP.")
                else:
                    print(f"❌ EMPTY -> SKIP.")

                # 2. Service Processing
                batch_data = []
                for item in services:
                    # Search in Indo, Report in English
                    indo_search_kw = self.KEYWORD_MAP.get(item, item)
                    specific_kw_indo = f"{indo_search_kw} {district}"
                    proxy_kw_indo = f"{indo_search_kw} Bali"
                    
                    resume_key = f"{item} {district}"
                    if resume_key in processed_keys: continue

                    row = {
                        'District': district, 
                        'Service Category': item, # English Output
                        'Search Keyword': specific_kw_indo, # Tracking only
                        'Data Source': "❌ Niche", 
                        'Forecast Index': 0, 
                        'Growth %': 0, 
                        'Avg Volume': 0,
                        'Market Status': "UNKNOWN", 
                        'Recommended Action': "Check"
                    }

                    if is_dead_region:
                        row.update({'Data Source': "❌ Skipped", 'Market Status': "➡️ STABLE", 'Recommended Action': "Maintain"})
                        print(".", end="")
                    else:
                        print(f"\n   🔍 {specific_kw_indo:<30}", end=" ")
                        
                        # --- ULTRA SAFE SLEEP ---
                        # Kita perlambat jadi 10-15 detik per request. 
                        # Ini satu-satunya cara menghindari 429 di IP yang "lelah".
                        time.sleep(random.uniform(10, 15)) 
                        
                        # Try Specific
                        data = self.fetch_safe([specific_kw_indo], context="Service")
                        use_proxy = False
                        
                        if not data.empty and specific_kw_indo in data.columns:
                            series = data[specific_kw_indo]
                            pred, growth, vol = self.calculate_forecast(series)
                            
                            if vol >= self.VOLUME_THRESHOLD:
                                row.update({
                                    'Data Source': "✅ Direct Data",
                                    'Forecast Index': round(pred, 1),
                                    'Growth %': round(growth, 1),
                                    'Avg Volume': round(vol, 1)
                                })
                                print(f"Growth: {int(growth)}% (Vol: {vol:.1f})", end="")
                            else:
                                use_proxy = True
                                print(f"Low Vol ({vol:.1f}) -> Proxy...", end="")
                        else:
                            use_proxy = True
                            print(f"No Data -> Proxy...", end="")
                        
                        # Fallback Proxy
                        if use_proxy:
                            time.sleep(random.uniform(5, 8)) # Extra sleep for proxy
                            proxy_data = self.fetch_safe([proxy_kw_indo], context="Proxy")
                            
                            if not proxy_data.empty and proxy_kw_indo in proxy_data.columns:
                                series = proxy_data[proxy_kw_indo]
                                pred, growth, _ = self.calculate_forecast(series)
                                row.update({
                                    'Data Source': "🔄 Proxy (Bali Trend)",
                                    'Forecast Index': round(pred, 1),
                                    'Growth %': round(growth, 1)
                                })
                                print(f" [PROXY] Growth: {int(growth)}%", end="")
                            else:
                                print(" Empty.", end="")
                        
                        # English Status Logic
                        g = row['Growth %']
                        ds = row['Data Source']
                        
                        if "✅" in ds or "🔄" in ds:
                            if g > 20: row['Market Status'], row['Recommended Action'] = "🔥 HIGH DEMAND", "Increase Inventory"
                            elif g < -15: row['Market Status'], row['Recommended Action'] = "❄️ LOW DEMAND", "Discount / Bundle"
                            else: row['Market Status'], row['Recommended Action'] = "➡️ STABLE", "Maintain Stock"
                        elif "Niche" in ds:
                             row['Market Status'], row['Recommended Action'] = "💤 LOW VOLUME", "Organic Only"

                    batch_data.append(row)
                    processed_keys.add(resume_key)

                # Save Batch
                if batch_data:
                    existing_data.extend(batch_data)
                    pd.DataFrame(existing_data).to_excel(self.OUTPUT_FILE, index=False)
        
        except KeyboardInterrupt:
            print("\n🛑 PAUSED BY USER. Data saved.")

        print(f"\n✅ DONE. Output: {self.OUTPUT_FILE}")

if __name__ == "__main__":
    forecaster = BaliDemandForecaster()
    forecaster.run()