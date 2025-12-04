import pandas as pd
import time
import random
import os
import warnings
from pytrends.request import TrendReq
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

class BaliDemandForecaster:
    def __init__(self):
        # Configuration
        self.BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.DISTRICT_FILE = os.path.join(self.BASE_DIR, 'data', 'raw', 'District_rows.csv')
        self.SERVICE_FILE = os.path.join(self.BASE_DIR, 'data', 'raw', 'ServiceSubCategory_rows.csv')
        self.OUTPUT_FILE = os.path.join(self.BASE_DIR, 'data', 'output', 'Bali_Forecast_Report.xlsx')
        
        # Thresholds
        self.VOLUME_THRESHOLD = 2.0  # Min avg volume to trust specific keyword
        self.REGION_THRESHOLD = 2.0  # Min region score to scan details
        
        # Exclusions (Non-Bali Districts)
        self.EXCLUDE_KEYWORDS = [
            'Strip', 'Summerlin', 'Henderson', 'Paradise', 
            'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
        ]

        # Smart Mapping: Service Name (DB) -> Search Query (Indo)
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
        """Loads and cleans raw data from CSV."""
        if not os.path.exists(self.DISTRICT_FILE) or not os.path.exists(self.SERVICE_FILE):
            raise FileNotFoundError("❌ Raw data not found in data/raw/")
        
        print("📂 Loading Data...")
        # Load Districts
        df_dist = pd.read_csv(self.DISTRICT_FILE)
        df_clean_dist = df_dist[~df_dist['district'].str.contains('|'.join(self.EXCLUDE_KEYWORDS), case=False, na=False)]
        districts = df_clean_dist['district'].unique().tolist()
        
        # Load Services
        df_serv = pd.read_csv(self.SERVICE_FILE)
        services = df_serv['name'].dropna().unique().tolist()
        clean_services = [s.split('/')[0].strip() for s in services]
        
        return districts, clean_services

    def fetch_safe(self, keywords):
        """Fetches data with exponential backoff for rate limits."""
        attempts = 0
        max_retries = 3
        while attempts < max_retries:
            try:
                self.pytrends.build_payload(keywords, timeframe='today 12-m', geo='ID')
                data = self.pytrends.interest_over_time()
                return data
            except Exception as e:
                if '429' in str(e):
                    wait = 60 + (attempts * 30)
                    print(f"🛑 Rate Limit (429). Sleeping {wait}s...", end="")
                    time.sleep(wait)
                else:
                    time.sleep(5)
                attempts += 1
                # Refresh session
                self.pytrends = TrendReq(hl='en-US', tz=360) 
        return pd.DataFrame()

    def calculate_forecast(self, series):
        """Applies Holt-Winters Exponential Smoothing."""
        try:
            series_monthly = series.resample('MS').mean()
            avg_vol = series_monthly.mean()
            
            # Remove partial last month
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
        
        # Smart Resume Logic
        existing_data = []
        processed_keys = set()
        if os.path.exists(self.OUTPUT_FILE):
            print(f"🔄 Resuming from: {self.OUTPUT_FILE}")
            try:
                df_exist = pd.read_excel(self.OUTPUT_FILE)
                existing_data = df_exist.to_dict('records')
                processed_keys = set([f"{row['Service Category']} {row['District']}" for row in existing_data])
            except: pass
        else:
            print(f"🆕 Creating new report: {self.OUTPUT_FILE}")

        print(f"🚀 ENGINE STARTED | Queue: {len(districts)} Districts")
        print("-" * 60)

        try:
            for district in districts:
                # 1. Region Check (Filter)
                proxy_kw = f"{district} Bali"
                if f"{services[0]} {district}" in processed_keys: continue 

                print(f"\n🌍 {district.upper()}...", end=" ")
                time.sleep(random.uniform(2, 4))
                region_data = self.fetch_safe([proxy_kw])
                
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
                    # Map to Indonesian Keyword
                    indo_item = self.KEYWORD_MAP.get(item, item)
                    specific_kw = f"{indo_item} {district}"
                    proxy_service_kw = f"{indo_item} Bali"
                    
                    resume_key = f"{item} {district}"
                    if resume_key in processed_keys: continue

                    # Data Holder
                    row = {
                        'District': district, 'Service Category': item,
                        'Data Source': "❌ Niche", 'Forecast Index': 0, 
                        'Growth %': 0, 'Avg Volume': 0,
                        'Market Status': "UNKNOWN", 'Recommended Action': "Check"
                    }

                    if is_dead_region:
                        row.update({'Data Source': "❌ Skipped", 'Market Status': "➡️ STABLE"})
                        print(".", end="")
                    else:
                        print(f"\n   🔍 {specific_kw:<30}", end=" ")
                        time.sleep(random.uniform(4, 7)) # Safe sleep for Biznet
                        
                        # Try Specific Keyword
                        data = self.fetch_safe([specific_kw])
                        use_proxy = False
                        
                        if not data.empty and specific_kw in data.columns:
                            series = data[specific_kw]
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
                        
                        # Fallback to Proxy
                        if use_proxy:
                            time.sleep(random.uniform(3, 5))
                            proxy_data = self.fetch_safe([proxy_service_kw])
                            if not proxy_data.empty and proxy_service_kw in proxy_data.columns:
                                series = proxy_data[proxy_service_kw]
                                pred, growth, _ = self.calculate_forecast(series)
                                row.update({
                                    'Data Source': "🔄 Proxy (Bali Trend)",
                                    'Forecast Index': round(pred, 1), # Trend reference
                                    'Growth %': round(growth, 1)
                                })
                                print(f" [PROXY] Growth: {int(growth)}%", end="")
                            else:
                                print(" Empty.", end="")
                        
                        # Determine Status
                        g = row['Growth %']
                        ds = row['Data Source']
                        
                        if "✅" in ds or "🔄" in ds:
                            if g > 20: row['Market Status'], row['Recommended Action'] = "🔥 HIGH DEMAND", "Increase Stock/Ads"
                            elif g < -15: row['Market Status'], row['Recommended Action'] = "❄️ LOW DEMAND", "Discount/Bundle"
                            else: row['Market Status'], row['Recommended Action'] = "➡️ STABLE", "Maintain"
                        elif ds == "❌ Niche":
                             row['Market Status'], row['Recommended Action'] = "💤 LOW VOL", "Organic Only"

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