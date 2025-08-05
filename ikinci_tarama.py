# -*- coding: utf-8 -*-
# FONALİZ DETAYLI ANALİZ SCRIPT'İ

import pandas as pd
import numpy as np
import time
import gspread
import pytz
import os
import json
import sys
from datetime import datetime, date
from tefas import Crawler
from tqdm import tqdm
import concurrent.futures
import warnings

warnings.filterwarnings('ignore')

# --- Sabitler ve Yapılandırma ---
GSPREAD_CREDENTIALS_SECRET = os.environ.get('GCP_SERVICE_ACCOUNT_KEY')
SHEET_ID = '1hSD4towyxKk9QHZFAcRlXy9NlLa_AyVrB9Jsy86ok14'
WORKSHEET_NAME_INPUT = 'haftalık'
WORKSHEET_NAME_OUTPUT = 'Fonanaliz'
TIMEZONE = pytz.timezone('Europe/Istanbul')
MAX_WORKERS = 10
ANALIZ_SURESI_AY = 3

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

def get_funds_from_gsheet(gc):
    print(f"\n'{WORKSHEET_NAME_INPUT}' sayfasından fon listesi okunuyor...")
    try:
        spreadsheet = gc.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME_INPUT)
        fund_codes = worksheet.col_values(1)[1:] # A2'den başla
        fund_codes = [code for code in fund_codes if code and str(code).strip()]
        if not fund_codes:
            print(f"⚠️ '{WORKSHEET_NAME_INPUT}' sayfasında fon kodu bulunamadı.")
            return []
        print(f"✅ {len(fund_codes)} adet fon kodu okundu.")
        return fund_codes
    except gspread.exceptions.WorksheetNotFound:
        print(f"❌ Hata: '{WORKSHEET_NAME_INPUT}' adlı çalışma sayfası bulunamadı.")
        return []
    except Exception as e:
        print(f"❌ Google Sheet'ten fon listesi okunurken hata: {e}")
        return []

def fetch_data_for_analysis(args):
    fon_kodu, start_date, end_date = args
    try:
        crawler = Crawler()
        df = crawler.fetch(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d"),
            name=fon_kodu,
            columns=["date", "price", "market_cap", "number_of_investors", "title"]
        )
        if df.empty: return fon_kodu, None, None
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        fon_adi = df['title'].iloc[0] if not df.empty else fon_kodu
        return fon_kodu, fon_adi, df.sort_values(by='date').reset_index(drop=True)
    except Exception:
        return fon_kodu, None, None

def calculate_metrics(df_fon):
    if df_fon is None or len(df_fon) < 10: return None
    df_fon['daily_return'] = df_fon['price'].pct_change().dropna()
    if df_fon['daily_return'].empty: return None
    
    getiri = (df_fon['price'].iloc[-1] / df_fon['price'].iloc[0]) - 1
    volatilite = df_fon['daily_return'].std() * np.sqrt(252)
    ortalama_gunluk_getiri = df_fon['daily_return'].mean()
    sharpe_orani = (ortalama_gunluk_getiri / df_fon['daily_return'].std()) * np.sqrt(252) if df_fon['daily_return'].std() != 0 else 0
    
    negatif_getiriler = df_fon[df_fon['daily_return'] < 0]['daily_return']
    downside_deviation = negatif_getiriler.std() * np.sqrt(252)
    sortino_orani = (ortalama_gunluk_getiri * 252) / downside_deviation if downside_deviation != 0 else 0
    
    return {
        'Getiri (%)': round(getiri * 100, 2),
        'Standart Sapma (Yıllık %)': round(volatilite * 100, 2),
        'Sharpe Oranı (Yıllık)': round(sharpe_orani, 2),
        'Sortino Oranı (Yıllık)': round(sortino_orani, 2),
        'Piyasa Değeri (TL)': df_fon['market_cap'].iloc[-1],
        'Yatırımcı Sayısı': df_fon['number_of_investors'].iloc[-1]
    }

def run_fonaliz_scan(gc, fon_listesi):
    start_time_main = time.time()
    print("\n" + "="*40)
    print("     DETAYLI FON ANALİZİ BAŞLATILIYOR")
    print(f"     {len(fon_listesi)} adet fon analiz edilecek...")
    print("="*40)

    end_date = datetime.now(TIMEZONE).date()
    start_date = end_date - pd.DateOffset(months=ANALIZ_SURESI_AY)
    
    tasks = [(fon_kodu, start_date, end_date) for fon_kodu in fon_listesi]
    analiz_sonuclari = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_fon = {executor.submit(fetch_data_for_analysis, task): task[0] for task in tasks}
        progress_bar = tqdm(concurrent.futures.as_completed(future_to_fon), total=len(tasks), desc="Detaylı Analiz")

        for future in progress_bar:
            fon_kodu, fon_adi, data = future.result()
            if data is not None:
                metrikler = calculate_metrics(data)
                if metrikler:
                    analiz_sonuclari.append({'Fon Kodu': fon_kodu, 'Fon Adı': fon_adi, **metrikler})

    print(f"\n✅ Analiz tamamlandı. Sonuçlar Google Sheets'e yazılıyor...")
    
    try:
        spreadsheet = gc.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME_OUTPUT)
        worksheet.clear()

        if analiz_sonuclari:
            df_sonuc = pd.DataFrame(analiz_sonuclari)
            df_sonuc.sort_values(by=['Sortino Oranı (Yıllık)', 'Sharpe Oranı (Yıllık)'], ascending=[False, False], inplace=True)
            
            sutun_sirasi = ['Fon Kodu', 'Fon Adı', 'Yatırımcı Sayısı', 'Piyasa Değeri (TL)', 'Sortino Oranı (Yıllık)', 'Sharpe Oranı (Yıllık)', 'Getiri (%)', 'Standart Sapma (Yıllık %)']
            df_sonuc = df_sonuc[sutun_sirasi]

            # A2'den başlayarak yaz
            worksheet.update('A2', [df_sonuc.columns.values.tolist()] + df_sonuc.values.tolist())
        else:
            print("ℹ️ Analiz edilecek veri bulunamadı, sayfa temizlendi.")

        # I1 hücresine zaman damgası yaz
        timestamp_str = "Son Tarama: " + datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
        worksheet.update_acell('I1', timestamp_str)
        
        body_resize = {"requests": [{"autoResizeDimensions": {"dimensions": {"sheetId": worksheet.id, "dimension": "COLUMNS"}}}]}
        spreadsheet.batch_update(body_resize)
        print("✅ Google Sheets güncellendi.")
    except Exception as e:
        print(f"❌ Google Sheets'e yazma hatası: {e}")

    print(f"--- Detaylı Analiz Bitti. Toplam Süre: {time.time() - start_time_main:.2f} saniye ---")

# --- ANA ÇALIŞTIRMA BLOĞU ---
if __name__ == "__main__":
    print("Fonaliz Detaylı Analiz Script'i Başlatıldı.")
    
    gc_auth = google_sheets_auth()
    if not gc_auth:
        sys.exit(1)

    fon_kodlari_listesi = get_funds_from_gsheet(gc_auth)

    if fon_kodlari_listesi:
        run_fonaliz_scan(gc_auth, fon_kodlari_listesi)
    else:
        print("Analiz edilecek fon bulunamadığı için işlem sonlandırıldı.")

    print("\n--- Tüm işlemler tamamlandı ---")