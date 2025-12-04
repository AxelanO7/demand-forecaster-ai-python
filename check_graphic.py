from pytrends.request import TrendReq
import pandas as pd

# Setup
pytrends = TrendReq(hl='en-US', tz=360)
kw = "Sewa Motor Kuta"

print(f"📊 Menganalisa Detail: '{kw}'")
try:
    pytrends.build_payload([kw], timeframe='today 12-m', geo='ID')
    data = pytrends.interest_over_time()
    
    if not data.empty:
        # Resample ke Bulanan biar gampang baca
        monthly = data[kw].resample('MS').mean()
        print("\n📅 Data Bulanan:")
        print(monthly)
        print(f"\n📈 Rata-rata Skor: {monthly.mean():.2f}")
    else:
        print("Data Kosong.")
except Exception as e:
    print(f"Error: {e}")