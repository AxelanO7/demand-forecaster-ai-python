from pytrends.request import TrendReq
import pandas as pd

# Setup Biznet Session
pytrends = TrendReq(hl='en-US', tz=360)

# Kita bandingkan 3 variasi keyword untuk KUTA
keywords = [
    "Motorbike Kuta",      # Keyword Script Anda (English)
    "Sewa Motor Kuta",     # Keyword Lokal (Indo)
    "Nightclub Kuta"       # Keyword Script Anda
]

print(f"📡 DEBUGGING BIZNET IP...")
print(f"Testing keywords: {keywords}\n")

try:
    pytrends.build_payload(keywords, timeframe='today 12-m', geo='ID')
    data = pytrends.interest_over_time()
    
    if data.empty:
        print("❌ HASIL: DATAFRAME KOSONG TOTAL.")
        print("Diagnosis: IP Anda sedang di-Shadow Ban oleh Google.")
        print("Solusi: Tunggu 30 menit atau Ganti IP VPN (Jepang/Australia).")
    else:
        print("✅ HASIL: DATA DITEMUKAN!")
        print("-" * 40)
        print(data.mean()) # Print rata-rata skor
        print("-" * 40)
        print("\n👇 CONTOH DATA (5 Baris Pertama):")
        print(data.head())
        
        # Analisis Cepat
        indo_score = data['Sewa Motor Kuta'].mean()
        eng_score = data['Motorbike Kuta'].mean()
        
        print(f"\n📊 ANALISIS:")
        print(f"- Sewa Motor Kuta (Indo): {indo_score:.2f}")
        print(f"- Motorbike Kuta (Eng)  : {eng_score:.2f}")
        
        if eng_score == 0 and indo_score > 0:
            print("👉 MASALAH: Keyword Bahasa Inggris tidak ada volume di Google Indonesia.")
            print("👉 SARAN: Ganti data input Anda ke Bahasa Indonesia.")
        elif eng_score == 0 and indo_score == 0:
            print("👉 MASALAH: IP Terblokir (Shadow Ban).")
        else:
            print("👉 AMAN: Kedua keyword valid. Silakan lanjut script utama.")

except Exception as e:
    print(f"❌ ERROR: {e}")