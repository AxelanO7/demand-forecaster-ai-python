# 🌴 Bali Tourism Demand Forecaster

**Automated AI Engine for Predicting Hyper-Local Tourism Demand in Bali.**

This project helps tourism businesses (Inventory Management) predict demand for specific services (e.g., "Motorbike Rental", "Spa") across different districts in Bali (e.g., "Kuta", "Ubud") using **Google Trends Data** and **Time Series Forecasting**.

## 🚀 Key Features

* **Hybrid Data Source:** Combines internal product taxonomy with external real-time market signals (Google Trends).
* **Smart Proxy Modeling:** Solves the "Cold Start / Low Volume" problem. If a specific keyword (e.g., "Fishing in Sidemen") has no data, the engine automatically falls back to regional trends (e.g., "Fishing in Bali") to provide a proxy forecast.
* **Auto-Translation Query:** Automatically maps English inventory names (e.g., "Nightclub") to local Indonesian search queries (e.g., "Club Malam") for maximum accuracy.
* **Turbo Skip Architecture:** intelligently detects quiet regions and skips unnecessary API calls to save time and API quota.
* **Self-Healing Resume:** The script saves progress in real-time. If interrupted (network error/manual stop), it resumes exactly where it left off.

## 🛠️ Tech Stack

* **Language:** Python 3.10+
* **Data Acquisition:** `pytrends` (Unofficial Google Trends API)
* **Forecasting:** `statsmodels` (Holt-Winters Exponential Smoothing)
* **Data Processing:** `pandas`

## ⚙️ How to Run

1.  **Clone the Repository**
    ```bash
    git clone [https://github.com/USERNAME/bali-demand-forecaster.git](https://github.com/USERNAME/bali-demand-forecaster.git)
    cd bali-demand-forecaster
    ```

2.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Prepare Data**
    Place your `District_rows.csv` and `ServiceSubCategory_rows.csv` into `data/raw/`.

4.  **Run the Engine**
    ```bash
    python src/forecaster.py
    ```

5.  **View Results**
    The forecast report will be generated at `data/output/Bali_Forecast_Report.xlsx`.

## 📊 Logic Flow

1.  **Region Check:** Is the district active digitally? (Score > 2.0).
2.  **Specific Query:** Check "Service + District" volume.
3.  **Fallback:** If volume is too low, fetch "Service + Bali" trend as proxy.
4.  **Forecast:** Predict Next Month Growth % using Exponential Smoothing.
5.  **Action Plan:** Generate recommendation (Stock Up / Discount / Maintain).

---
*Built as a Strategic AI Tool for Inventory Optimization.*