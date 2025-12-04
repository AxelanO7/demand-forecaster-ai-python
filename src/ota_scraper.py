import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import random
import os
from datetime import datetime, timedelta

# --- KONFIGURASI ---
DISTRICT_FILE = 'data/raw/District_rows.csv' # Sesuaikan path folder Anda
OUTPUT_FILE = 'data/output/Bali_OTA_Supply_Data.xlsx'

# Filter Wilayah Non-Bali
EXCLUDE_KEYWORDS = [
    'Strip', 'Summerlin', 'Henderson', 'Paradise', 
    'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
]

# TANGGAL CEK: Kita cek availability untuk Weekend di Bulan Depan
# (Misal: Checkin 30 hari lagi, Checkout 31 hari lagi)
CHECKIN_DATE = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
CHECKOUT_DATE = (datetime.now() + timedelta(days=31)).strftime("%Y-%m-%d")

async def get_booking_count(page, district):
    """
    Membuka Booking.com dan mengambil jumlah properti yang tersedia.
    """
    # URL Pattern Booking.com (Direct Search)
    url = f"https://www.booking.com/searchresults.en-gb.html?ss={district}, Bali, Indonesia&checkin={CHECKIN_DATE}&checkout={CHECKOUT_DATE}&group_adults=2&no_rooms=1&group_children=0"
    
    try:
        # Buka halaman
        await page.goto(url, timeout=60000) # 60 detik timeout
        
        # Tunggu loading selesai (kadang ada captcha ringan)
        # Kita tunggu elemen H1 yang berisi jumlah properti
        try:
            # Selector H1 biasanya berisi: "Ubud: 1,230 properties found"
            h1_element = await page.wait_for_selector('h1', timeout=10000)
            text = await h1_element.inner_text()
            
            # Parsing Text: "Ubud: 1,230 properties found" -> ambil angka
            # Hapus karakter non-digit
            import re
            numbers = re.findall(r'\d+', text.replace(',', ''))
            
            if numbers:
                # Angka biasanya yang terbesar (misal ada "2 adults" vs "1200 properties")
                count = max([int(n) for n in numbers])
                return count, text
            else:
                return 0, "No Number Found"
                
        except Exception:
            # Kadang layout beda, Booking.com sering A/B testing
            return 0, "Layout Changed/Captcha"

    except Exception as e:
        return 0, f"Error Load: {str(e)}"

async def main():
    # 1. Load Data Wilayah
    if not os.path.exists(DISTRICT_FILE):
        print("❌ File District tidak ditemukan.")
        return

    df_dist = pd.read_csv(DISTRICT_FILE)
    df_clean = df_dist[~df_dist['district'].str.contains('|'.join(EXCLUDE_KEYWORDS), case=False, na=False)]
    districts = df_clean['district'].unique().tolist()
    
    print(f"🚀 OTA SCRAPER STARTED | Target: {len(districts)} Districts")
    print(f"📅 Checking for: {CHECKIN_DATE}")
    print("-" * 60)

    results = []
    
    # Smart Resume
    if os.path.exists(OUTPUT_FILE):
        print(f"🔄 Resuming from {OUTPUT_FILE}...")
        df_exist = pd.read_excel(OUTPUT_FILE)
        results = df_exist.to_dict('records')
        processed_districts = [r['District'] for r in results]
    else:
        processed_districts = []

    # Mulai Browser (Headless = False supaya terlihat seperti manusia & bisa debug)
    async with async_playwright() as p:
        # Gunakan Chrome/Chromium
        browser = await p.chromium.launch(headless=False) 
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        for district in districts:
            if district in processed_districts:
                continue

            print(f"🌍 Checking Supply: {district.upper()}...", end=" ")
            
            # Random Sleep agar tidak diblokir (Penting untuk OTA!)
            await asyncio.sleep(random.uniform(3, 6))
            
            count, raw_text = await get_booking_count(page, district)
            
            status = "UNKNOWN"
            if count > 1000: status = "🟢 HIGH SUPPLY"
            elif count > 200: status = "🟡 MEDIUM SUPPLY"
            elif count > 0: status = "🔴 LOW SUPPLY (SCARCITY)"
            else: status = "❌ ERROR/BLOCKED"

            print(f"-> {count} Properties Found.")

            results.append({
                'District': district,
                'Checkin Date': CHECKIN_DATE,
                'Available Properties': count,
                'Raw Text': raw_text,
                'Supply Status': status,
                'Scraped At': datetime.now().strftime("%Y-%m-%d %H:%M")
            })

            # Save Real-time
            pd.DataFrame(results).to_excel(OUTPUT_FILE, index=False)

        await browser.close()
        print(f"\n✅ Selesai! Data tersimpan di {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())