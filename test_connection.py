from pytrends.request import TrendReq
import time

print("📡 Sedang mengetes koneksi dari MacBook Anda...")

# Cek apakah request tembus
try:
    pytrends = TrendReq(hl='en-US', tz=360)
    keywords = ["Kuta Bali", "Sewa Motor Bali"]
    
    pytrends.build_payload(keywords, timeframe='today 12-m', geo='ID')
    data = pytrends.interest_over_time()
    
    if not data.empty:
        print("\n✅ SUKSES! Google Trends memberikan data:")
        print(data.head())
        print("\nKesimpulan: IP Aman. Silakan jalankan script utama.")
    else:
        print("\n❌ DATA KOSONG. (Silent Blocking)")
        print("Solusi: Ganti Server VPN Desktop atau Reset Hotspot.")

except Exception as e:
    print(f"\n❌ ERROR KERAS (429): {e}")
    print("Solusi: IP Anda diblokir. Matikan Wi-Fi, nyalakan Hotspot HP.")