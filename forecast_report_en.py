def main():
    pytrends = TrendReq(hl='en-US', tz=360)
    
    try:
        districts, services = load_data()
    except Exception as e:
        print(e)
        return

    # --- SMART RESUME & FAIL-SAFE CREATE ---
    existing_data = []
    processed_keys = set()
    
    if os.path.exists(OUTPUT_FILE):
        print(f"🔄 Resuming from: {OUTPUT_FILE}")
        try:
            df_exist = pd.read_excel(OUTPUT_FILE)
            existing_data = df_exist.to_dict('records')
            processed_keys = set([f"{row['Service Category']} {row['District']}" for row in existing_data])
        except: pass
    else:
        print(f"🆕 Creating new report: {OUTPUT_FILE}")
        # FAIL-SAFE: Buat file kosong header dulu agar file fisik tercipta
        dummy_df = pd.DataFrame(columns=[
            'District', 'Service Category', 'Data Source', 'Forecast Index', 
            'Growth %', 'Avg Volume', 'Market Status', 'Recommended Action'
        ])
        dummy_df.to_excel(OUTPUT_FILE, index=False)

    print(f"🚀 ENGINE STARTED (ENGLISH REPORT MODE)")
    print("⏳ Warming up (10s)...") 
    time.sleep(10) # Jeda awal agar tidak kaget
    print("-" * 60)
    
    try:
        for district in districts:
            # 1. Region Check
            proxy_kw = f"{district} Bali"
            if f"{services[0]} {district}" in processed_keys: continue 

            print(f"\n🌍 {district.upper()}...", end=" ")
            
            # Random sleep sebelum request pertama di loop
            time.sleep(random.uniform(5, 8))
            
            region_data = fetch_safe(pytrends, [proxy_kw])
            
            is_dead_region = True
            if not region_data.empty and proxy_kw in region_data.columns:
                score = region_data[proxy_kw].mean()
                if score >= REGION_THRESHOLD:
                    is_dead_region = False
                    print(f"✅ ACTIVE (Avg: {score:.1f})")
                else:
                    print(f"❌ QUIET (Avg: {score:.1f}) -> SKIP.")
            else:
                print(f"❌ EMPTY -> SKIP.")

            # 2. Service Check
            batch_data = []
            for item in services:
                indo_item = KEYWORD_MAP.get(item, item)
                
                specific_kw = f"{indo_item} {district}"
                proxy_service_kw = f"{indo_item} Bali" 
                
                # Cek Resume (Penting!)
                resume_key = f"{item} {district}" # Pakai nama asli untuk key
                if resume_key in processed_keys: 
                    continue

                if is_dead_region:
                    batch_data.append({
                        'District': district, 
                        'Service Category': item, 
                        'Data Source': "❌ Skipped (Quiet Region)", 
                        'Forecast Index': 0, 'Growth %': 0, 'Avg Volume': 0,
                        'Market Status': "➡️ STABLE", 'Recommended Action': "Maintain Visibility"
                    })
                    print(".", end="")
                else:
                    print(f"\n   🔍 {specific_kw:<30}", end=" ")
                    time.sleep(random.uniform(5, 10)) # Sleep standar
                    
                    data = fetch_safe(pytrends, [specific_kw])
                    
                    final_growth = 0
                    forecast_val = 0
                    vol = 0
                    data_source = "❌ Niche Market"
                    status = "UNKNOWN"
                    action = "Manual Check"
                    
                    use_proxy = False
                    
                    if not data.empty and specific_kw in data.columns:
                        series = data[specific_kw]
                        pred, growth, vol = get_forecast_logic(series)
                        
                        if vol >= VOLUME_THRESHOLD:
                            final_growth = growth
                            forecast_val = pred
                            data_source = "✅ Direct Data"
                            print(f"Growth: {int(growth)}% (Vol: {vol:.1f})", end="")
                        else:
                            use_proxy = True
                            print(f"Low Vol ({vol:.1f}) -> Proxy...", end="")
                    else:
                        use_proxy = True
                        print(f"No Data -> Proxy...", end="")
                        
                    if use_proxy:
                        time.sleep(random.uniform(4, 6))
                        proxy_data = fetch_safe(pytrends, [proxy_service_kw])
                        
                        if not proxy_data.empty and proxy_service_kw in proxy_data.columns:
                            series = proxy_data[proxy_service_kw]
                            pred, growth, p_vol = get_forecast_logic(series)
                            final_growth = growth
                            forecast_val = pred 
                            data_source = "🔄 Proxy (Bali Trend)"
                            print(f" [PROXY] Growth: {int(growth)}%", end="")
                        else:
                            print(" Proxy Empty.", end="")

                    if "✅" in data_source or "🔄" in data_source:
                        if final_growth > 20: status, action = "🔥 HIGH DEMAND", "Increase Inventory / Ads"
                        elif final_growth < -15: status, action = "❄️ LOW DEMAND", "Discount / Bundle Offer"
                        else: status, action = "➡️ STABLE", "Maintain Stock"
                    elif data_source == "❌ Niche Market":
                         status, action = "💤 LOW VOLUME", "Organic Growth Only"

                    batch_data.append({
                        'District': district, 
                        'Service Category': item, 
                        'Data Source': data_source,
                        'Forecast Index': round(forecast_val, 1),
                        'Growth %': round(final_growth, 1),
                        'Avg Volume': round(vol if not use_proxy else 0, 1),
                        'Market Status': status, 
                        'Recommended Action': action
                    })

                # Tambahkan ke processed keys agar tidak double
                processed_keys.add(resume_key)

            # SAVE PER DISTRICT
            if batch_data:
                existing_data.extend(batch_data)
                pd.DataFrame(existing_data).to_excel(OUTPUT_FILE, index=False)
            
    except KeyboardInterrupt:
        print("\n🛑 PAUSED. Data saved safely.")

    print(f"\n✅ FINISHED. Report saved to: {OUTPUT_FILE}")