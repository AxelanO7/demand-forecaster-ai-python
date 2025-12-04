import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import random
import os
from datetime import datetime, timedelta

# --- KONFIGURASI PATH DINAMIS ---
# Ini akan mencari folder 'data' relatif terhadap lokasi script ini
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DISTRICT_FILE = os.path.join(BASE_DIR, 'data', 'raw', 'District_rows.csv')
OUTPUT_FILE = os.path.join(BASE_DIR, 'data', 'output', 'Bali_OTA_Supply_Data.xlsx')

# Filter Wilayah Non-Bali
EXCLUDE_KEYWORDS = [
    'Strip', 'Summerlin', 'Henderson', 'Paradise', 
    'Spring', 'Enterprise', 'Downtown', 'Winchester', 'Sunrise', 'Whitney'
]

# TANGGAL CEK: Weekend Bulan Depan (30 hari dari sekarang)
CHECKIN_DATE = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
CHECKOUT_DATE = (datetime.now() + timedelta(days=31)).strftime("%Y-%m-%d")

async def get_booking_count(page, district):
    """
    Scrape jumlah properti tersedia di Booking.com
    """
    # URL Search Langsung
    url = f"https://www.booking.com/searchresults.en-gb.html?ss={district}, Bali, Indonesia&checkin={CHECKIN_DATE}&checkout={CHECKOUT_DATE}&group_adults=2&no_rooms=1&group_children=0"
    
    try:
        await page.goto(url, timeout=60000) # 60s timeout
        
        # Tunggu elemen H1 (biasanya berisi count)
        try:
            # Selector H1 di Booking.com berisi text seperti "Ubud: 1,230 properties found"
            h1_element = await page.wait_for_selector('h1', timeout=10000)
            text = await h1_element.inner_text()
            
            # Ambil angka dari teks
            import re
            numbers = re.findall(r'\d+', text.replace(',', ''))
            
            if numbers:
                # Ambil angka terbesar (untuk menghindari angka kecil seperti "2 adults")
                count = max([int(n) for n in numbers])
                return count, text
            else:
                return 0, "No Number Found"
                
        except Exception:
            return 0, "Layout Changed/Captcha"

    except Exception as e:
        return 0, f"Error Load: {str(e)}"

async def main():
    # 1. Load Data
    if not os.path.exists(DISTRICT_FILE):
        print(f"❌ File District tidak ditemukan di: {DISTRICT_FILE}")
        print("👉 Pastikan Anda sudah membuat folder 'data/raw' dan menaruh CSV di sana.")
        return

    df_dist = pd.read_csv(DISTRICT_FILE)
    df_clean = df_dist[~df_dist['district'].str.contains('|'.join(EXCLUDE_KEYWORDS), case=False, na=False)]
    districts = df_clean['district'].unique().tolist()
    
    print(f"🚀 OTA SCRAPER STARTED | Target: {len(districts)} Districts")
    print(f"📂 Reading from: {DISTRICT_FILE}")
    print(f"📅 Check-in Date: {CHECKIN_DATE}")
    print("-" * 60)

    results = []
    processed_districts = []

    # Smart Resume
    if os.path.exists(OUTPUT_FILE):
        print(f"🔄 Resuming from {OUTPUT_FILE}...")
        try:
            df_exist = pd.read_excel(OUTPUT_FILE)
            results = df_exist.to_dict('records')
            processed_districts = [r['District'] for r in results]
        except: pass
    
    # Jalankan Browser (Headless=False agar terlihat seperti manusia)
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        for district in districts:
            if district in processed_districts:
                continue

            print(f"🌍 Checking Supply: {district.upper()}...", end=" ")
            
            # Random sleep wajib untuk OTA
            await asyncio.sleep(random.uniform(4, 7))
            
            count, raw_text = await get_booking_count(page, district)
            
            status = "UNKNOWN"
            if count > 1000: status = "🟢 HIGH SUPPLY"
            elif count > 200: status = "🟡 MEDIUM SUPPLY"
            elif count > 0: status = "🔴 LOW SUPPLY (SCARCITY)"
            else: status = "❌ ERROR/BLOCKED"

            print(f"-> {count} Properties.")

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