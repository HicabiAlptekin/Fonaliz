# -*- coding: utf-8 -*-
# OTOFON HAFTALIK İVMELENME TARAMA SCRIPT'İ

import pandas as pd
import numpy as np
import time
import gspread
import pytz
import os
import json
import sys
from datetime import datetime, timedelta, date
from tefas import Crawler
from tqdm import tqdm
import concurrent.futures
import warnings

warnings.filterwarnings('ignore')

# --- Sabitler ve Yapılandırma ---
GSPREAD_CREDENTIALS_SECRET = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
TAKASBANK_EXCEL_URL = 'https://www.takasbank.com.tr/plugins/ExcelExportTefasFundsTradingInvestmentPlatform?language=tr'
SHEET_ID = '1hSD4towyxKk9QHZFAcRlXy9NlLa_AyVrB9Jsy86ok14'
WORKSHEET_NAME_WEEKLY = 'haftalık'
TIMEZONE = pytz.timezone('Europe/Istanbul')
MAX_WORKERS = 10
NUM_WEEKS_TO_SCAN = 2 # Kaç hafta geriye dönük taranacak

# --- Yardımcı Fonksiyonlar ---
def google_sheets_auth():
    print("\nGoogle Sheets için kimlik doğrulaması yapılıyor...")
    try:
        if not GSPREAD_CREDENTIALS_SECRET:
            print("❌ Hata: GCP_SERVICE_ACCOUNT_KEY secret bulunamadı.")
            sys.exit(1)
        creds_json = json.loads(GSPREAD_CREDENTIALS_SECRET)
        gc = gspread.service_account_from_dict(creds_json)
        print("✅ Kimlik doğrulama başarılı.")
        return gc
    except Exception as e:
        print(f"❌ Kimlik doğrulama sırasında hata oluştu: {e}")
        sys.exit(1)

def load_takasbank_fund_list():
    print(f"Takasbank'tan güncel fon listesi yükleniyor...")
    try:
        df_excel = pd.read_excel(TAKASBANK_EXCEL_URL, engine='openpyxl')
        df_data = df_excel[['Fon Adı', 'Fon Kodu']].copy()
        df_data['Fon Kodu'] = df_data['Fon Kodu'].astype(str).str.strip().str.upper()
        df_data.dropna(subset=['Fon Kodu'], inplace=True)
        df_data = df_data[df_data['Fon Kodu'] != '']
        print(f"✅ {len(df_data)} adet fon bilgisi okundu.")
        return df_data
    except Exception as e:
        print(f"❌ Takasbank Excel yükleme hatası: {e}")
        return pd.DataFrame()

def get_price_on_or_before(df_fund_history, target_date: date):
    if df_fund_history is None or df_fund_history.empty or target_date is None: return np.nan
    df_filtered = df_fund_history[df_fund_history['date'] <= target_date].copy()
    if not df_filtered.empty: return df_filtered.sort_values(by='date', ascending=False)['price'].iloc[0]
    return np.nan

def calculate_change(current_price, past_price):
    if pd.isna(current_price) or pd.isna(past_price) or past_price is None or current_price is None: return np.nan
    try:
        current_price_float, past_price_float = float(current_price), float(past_price)
        if past_price_float == 0: return np.nan
        return ((current_price_float - past_price_float) / past_price_float) * 100
    except (ValueError, TypeError): return np.nan

def fetch_data_for_fund_parallel(args):
    fon_kodu, start_date_overall, end_date_overall = args
    try:
        crawler = Crawler()
        df = crawler.fetch(
            start=start_date_overall.strftime("%Y-%m-%d"),
            end=end_date_overall.strftime("%Y-%m-%d"),
            name=fon_kodu,
            columns=["date", "price", "title"]
        )
        if df.empty: return fon_kodu, None
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        return fon_kodu, df.sort_values(by='date').reset_index(drop=True)
    except Exception:
        return fon_kodu, None

def run_acceleration_scan_and_write_to_sheet(gc, num_weeks: int):
    start_time_main = time.time()
    today = datetime.now(TIMEZONE).date()
    all_fon_data_df = load_takasbank_fund_list()

    if all_fon_data_df.empty:
        print("❌ Taranacak fon listesi alınamadı. İşlem durduruldu.")
        return

    print("\n" + "="*40)
    print("     HAFTALIK İVMELENME TARAMASI BAŞLATILIYOR")
    print("="*40)

    genel_veri_cekme_baslangic_tarihi = today - timedelta(days=(num_weeks * 7) + 21)
    tasks = [(fon_kodu, genel_veri_cekme_baslangic_tarihi, today) for fon_kodu in all_fon_data_df['Fon Kodu'].unique()]
    
    weekly_results_dict = {}
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_fon = {executor.submit(fetch_data_for_fund_parallel, args): args[0] for args in tasks}
        progress_bar = tqdm(concurrent.futures.as_completed(future_to_fon), total=len(tasks), desc="Haftalık Tarama")

        for future in progress_bar:
            fon_kodu, fund_history = future.result()
            if fund_history is None or fund_history.empty: continue

            fon_adi = all_fon_data_df.loc[all_fon_data_df['Fon Kodu'] == fon_kodu, 'Fon Adı'].iloc[0]
            current_fon_data = {'Fon Kodu': fon_kodu, 'Fon Adı': fon_adi}
            
            weekly_changes = []
            current_week_end_date = today
            for i in range(num_weeks):
                current_week_start_date = current_week_end_date - timedelta(days=7)
                price_end = get_price_on_or_before(fund_history, current_week_end_date)
                price_start = get_price_on_or_before(fund_history, current_week_start_date)
                
                col_name = f"Hafta_{i+1}_Getiri"
                change = calculate_change(price_end, price_start)
                current_fon_data[col_name] = change
                weekly_changes.append(change)
                current_week_end_date = current_week_start_date
            
            valid_changes = [c for c in weekly_changes if pd.notna(c)]
            if len(valid_changes) == num_weeks:
                 current_fon_data['Toplam_Getiri'] = sum(valid_changes)
                 weekly_results_dict[fon_kodu] = current_fon_data

    results_df = pd.DataFrame(list(weekly_results_dict.values()))
    
    if results_df.empty:
        print("\nAnaliz edilecek yeterli veri bulunamadı.")
        return

    # Filtreleme
    filtrelenmis_df = results_df[results_df['Toplam_Getiri'] >= 2].copy()
    filtrelenmis_df.sort_values(by='Toplam_Getiri', ascending=False, inplace=True)
    
    print(f"\n✅ Haftalık tarama tamamlandı. {len(filtrelenmis_df)} fon filtreden geçti.")
    print(f"Sonuçlar Google Sheets'teki '{WORKSHEET_NAME_WEEKLY}' sayfasına yazılıyor...")

    try:
        spreadsheet = gc.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME_WEEKLY)
        worksheet.clear()
        
        # Sadece istenen sütunları yaz
        output_columns = ['Fon Kodu', 'Fon Adı', 'Hafta_1_Getiri', 'Hafta_2_Getiri', 'Toplam_Getiri']
        df_to_write = filtrelenmis_df[output_columns]
        
        worksheet.update([df_to_write.columns.values.tolist()] + df_to_write.values.tolist())
        
        body_resize = {"requests": [{"autoResizeDimensions": {"dimensions": {"sheetId": worksheet.id, "dimension": "COLUMNS"}}}]}
        spreadsheet.batch_update(body_resize)
        print("✅ Google Sheets güncellendi.")
    except Exception as e:
        print(f"❌ Google Sheets'e yazma hatası: {e}")

    print(f"--- Haftalık Tarama Bitti. Toplam Süre: {time.time() - start_time_main:.2f} saniye ---")

# --- ANA ÇALIŞTIRMA BLOĞU ---
if __name__ == "__main__":
    print("OtoFon Haftalık İvmelenme Taraması Başlatıldı.")
    
    gc_auth = google_sheets_auth()
    if not gc_auth:
        sys.exit(1)

    run_acceleration_scan_and_write_to_sheet(gc_auth, num_weeks=NUM_WEEKS_TO_SCAN)

    print("\n--- Tüm işlemler tamamlandı ---")