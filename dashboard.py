import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Bali Demand Forecaster", page_icon="🌴", layout="wide")

# --- JUDUL & HEADER ---
st.title("🌴 Bali Tourism Demand Forecaster")
st.markdown("### AI-Powered Prediction for Inventory Management")
st.markdown("---")

# --- LOAD DATA ---
# Ganti nama file ini sesuai nama file output Excel Anda yang terakhir
FILE_PATH = 'Bali_Smart_Forecast.xlsx' 

@st.cache_data
def load_data():
    if os.path.exists(FILE_PATH):
        df = pd.read_excel(FILE_PATH)
        return df
    return None

df = load_data()

if df is None:
    st.error(f"File '{FILE_PATH}' tidak ditemukan! Pastikan file Excel ada di folder yang sama.")
    st.stop()

# --- SIDEBAR (FILTER) ---
st.sidebar.header("🔍 Filter Options")

# Filter Wilayah
all_districts = ["All"] + sorted(df['District'].unique().tolist())
selected_district = st.sidebar.selectbox("Pilih Wilayah (District):", all_districts)

# Filter Kategori Service
all_services = ["All"] + sorted(df['Service'].unique().tolist())
selected_service = st.sidebar.selectbox("Pilih Layanan (Service):", all_services)

# Filter Status Demand
all_status = ["All"] + sorted(df['Status'].unique().tolist())
selected_status = st.sidebar.selectbox("Filter Status:", all_status)

# --- FILTER LOGIC ---
df_filtered = df.copy()

if selected_district != "All":
    df_filtered = df_filtered[df_filtered['District'] == selected_district]

if selected_service != "All":
    df_filtered = df_filtered[df_filtered['Service'] == selected_service]

if selected_status != "All":
    df_filtered = df_filtered[df_filtered['Status'] == selected_status]

# --- MAIN DASHBOARD (KPI) ---
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Items Analyzed", len(df_filtered))

with col2:
    # Hitung rata-rata pertumbuhan
    avg_growth = df_filtered['Growth Forecast %'].mean()
    st.metric("Avg. Market Growth", f"{avg_growth:.1f}%", delta_color="normal")

with col3:
    # Hitung item yang HOT
    hot_items = len(df_filtered[df_filtered['Status'] == '🔥 HOT'])
    st.metric("High Demand Opportunities", hot_items, delta=f"{hot_items} Items")

st.markdown("---")

# --- CHARTS ---
col_chart1, col_chart2 = st.columns([2, 1])

with col_chart1:
    st.subheader("📈 Top Growth Opportunities")
    # Bar Chart untuk Growth tertinggi
    top_growth = df_filtered.sort_values(by='Growth Forecast %', ascending=False).head(10)
    if not top_growth.empty:
        fig_bar = px.bar(
            top_growth, 
            x='Search Keyword', 
            y='Growth Forecast %',
            color='Status',
            color_discrete_map={'🔥 HOT': '#FF4B4B', '➡️ STABLE': '#FFA500', '❄️ COLD': '#00B4D8'},
            title="Top 10 Highest Predicted Demand (Next Month)"
        )
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Data tidak cukup untuk menampilkan grafik.")

with col_chart2:
    st.subheader("📊 Demand Distribution")
    # Pie Chart
    if not df_filtered.empty:
        status_counts = df_filtered['Status'].value_counts().reset_index()
        status_counts.columns = ['Status', 'Count']
        fig_pie = px.pie(
            status_counts, 
            values='Count', 
            names='Status', 
            hole=0.4,
            color='Status',
            color_discrete_map={'🔥 HOT': '#FF4B4B', '➡️ STABLE': '#FFA500', '❄️ COLD': '#00B4D8'}
        )
        st.plotly_chart(fig_pie, use_container_width=True)

# --- DATA TABLE ---
st.subheader("📋 Detailed Data View")
st.dataframe(
    df_filtered[['District', 'Service', 'Growth Forecast %', 'Status', 'Action Plan', 'Data Source']],
    use_container_width=True,
    hide_index=True
)

# Tombol Download
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

csv = convert_df(df_filtered)
st.download_button(
    label="📥 Download Filtered Data as CSV",
    data=csv,
    file_name='filtered_forecast.csv',
    mime='text/csv',
)