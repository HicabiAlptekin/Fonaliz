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
    print("\n[ADIM 1/5] Google Sheets için kimlik doğrulaması yapılıyor...")
    try:
        if not GSPREAD_CREDENTIALS_SECRET:
            print("❌ Hata: GCP_SERVICE_ACCOUNT_KEY secret bulunamadı. Kimlik doğrulama başarısız.")
            sys.exit(1)
        creds_json = json.loads(GSPREAD_CREDENTIALS_SECRET)
        gc = gspread.service_account_from_dict(creds_json)
        print("✅ Kimlik doğrulama başarılı.")
        return gc
    except Exception as e:
        print(f"❌ Kimlik doğrulama sırasında hata oluştu: {e}. İşlem durduruldu.")
        sys.exit(1)

def load_takasbank_fund_list():
    print(f"\n[ADIM 2/5] Takasbank'tan güncel fon listesi yükleniyor...")
    try:
        df_excel = pd.read_excel(TAKASBANK_EXCEL_URL, engine='openpyxl')
        df_data = df_excel[['Fon Adı', 'Fon Kodu']].copy()
        df_data['Fon Kodu'] = df_data['Fon Kodu'].astype(str).str.strip().str.upper()
        df_data.dropna(subset=['Fon Kodu'], inplace=True)
        df_data = df_data[df_data['Fon Kodu'] != '']
        print(f"✅ {len(df_data)} adet fon bilgisi başarıyla okundu.")
        return df_data
    except Exception as e:
        print(f"❌ Takasbank Excel yükleme hatası: {e}. Fon listesi alınamadı.")
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
    fon_kodu, start_date_overall, end_date_overall, bekleme_suresi = args
    try:
        if bekleme_suresi > 0:
            time.sleep(random.uniform(bekleme_suresi, bekleme_suresi + 1.0))

        crawler = Crawler()
        df = crawler.fetch(
            start=start_date_overall.strftime("%Y-%m-%d"),
            end=end_date_overall.strftime("%Y-%m-%d"),
            name=fon_kodu,
            columns=["date", "price", "title"]
        )
        if df.empty:
            return fon_kodu, None, "Veri bulunamadı veya boş döndü."
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        return fon_kodu, df.sort_values(by='date').reset_index(drop=True), None
    except Exception as e:
        error_message = str(e)
        return fon_kodu, None, error_message

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
    all_fon_codes = all_fon_data_df['Fon Kodu'].unique().tolist()
    
    successful_funds_data = {}
    failed_funds_by_stage = {}
    
    # Tarama Aşamaları Tanımları
    stages = [
        {"name": "1. Aşama (Hızlı)", "max_workers": 10, "bekleme_suresi": 0},
        {"name": "2. Aşama (Orta)", "max_workers": 4, "bekleme_suresi": 1.5},
        {"name": "3. Aşama (Yavaş)", "max_workers": 2, "bekleme_suresi": 3.0}
    ]

    current_fon_codes_to_process = all_fon_codes

    for i, stage in enumerate(stages):
        if not current_fon_codes_to_process:
            print(f"Tüm fonlar başarıyla işlendi, {stage['name']} atlanıyor.")
            break

        print(f"\n[ADIM 3/{i+1}/5] {stage['name']} başlatılıyor ({len(current_fon_codes_to_process)} fon için)...")
        
        tasks = [(fon_kodu, genel_veri_cekme_baslangic_tarihi, today, stage["bekleme_suresi"]) for fon_kodu in current_fon_codes_to_process]
        
        stage_successful_funds = {}
        stage_failed_funds = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=stage["max_workers"]) as executor:
            future_to_fon = {executor.submit(fetch_data_for_fund_parallel, args): args[0] for args in tasks}
            progress_bar = tqdm(concurrent.futures.as_completed(future_to_fon), total=len(tasks), desc=f"{stage['name']} Tarama")

            for future in progress_bar:
                fon_kodu, fund_history, error_message = future.result()
                if error_message is None: # Başarılı
                    stage_successful_funds[fon_kodu] = fund_history
                else: # Başarısız
                    stage_failed_funds[fon_kodu] = error_message
        
        successful_funds_data.update(stage_successful_funds)
        failed_funds_by_stage[stage["name"]] = stage_failed_funds
        current_fon_codes_to_process = list(stage_failed_funds.keys()) # Bir sonraki aşama için başarısız olanları al

        print(f"✅ {stage['name']} tamamlandı. Başarılı: {len(stage_successful_funds)}, Başarısız: {len(stage_failed_funds)}")

    # Tüm aşamalardan gelen başarılı fonları işleme
    weekly_results_dict = {}
    print("\n[ADIM 4/5] Başarılı fonların haftalık getirileri hesaplanıyor...")
    for fon_kodu, fund_history in successful_funds_data.items():
        if fund_history is None or fund_history.empty: continue # Should not happen if successful_funds_data only contains successful ones

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
    
    results_df = pd.DataFrame(list(weekly_results_dict.values()))
    
    if results_df.empty:
        print("\nAnaliz edilecek yeterli veri bulunamadı. İşlem durduruldu.")
        # Özet Raporu (Bu durumda da gösterilmeli)
        toplam_fon_sayisi = len(all_fon_codes)
        basarili_fon_sayisi = len(successful_funds_data)
        nihai_basarisiz_fonlar = current_fon_codes_to_process # Son aşamada kalan başarısızlar
        
        ozet_metni = f"""
--- Analiz Özet Raporu ({datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}) ---
Toplam Analize Alınan Fon Sayısı: {toplam_fon_sayisi}
Başarıyla Analiz Edilen Fon Sayısı: {basarili_fon_sayisi}
Nihai Başarısız Fon Sayısı: {len(nihai_basarisiz_fonlar)}

Aşamalara Göre Başarısız Olan Fonlar:
"""
        for stage_name, failed_funds in failed_funds_by_stage.items():
            ozet_metni += f"- {stage_name}: {len(failed_funds)} fon başarısız oldu.\n"
            if failed_funds:
                ozet_metni += "  Detaylar:\n"
                for fon_kodu, sebep in failed_funds.items():
                    ozet_metni += f"  - {fon_kodu}: {sebep}\n"
            else:
                ozet_metni += "  Tüm fonlar bu aşamada başarılı oldu.\n"

        ozet_metni += """
Olası Genel Hata Nedenleri:
- TEFAS web sitesi, kısa sürede yapılan çok sayıda isteği engellemek için 'Rate Limiting' (erişim kısıtlaması) uygulamaktadır.
- Veri çekme işlemi sırasında internet bağlantısı kaynaklı zaman aşımları ('Timeout') yaşanmış olabilir.
- Listede yer alan bazı fon kodları güncel olmayabilir veya TEFAS platformunda bulunamayabilir.
"""
        print(ozet_metni)
        ozet_dosya_adi = "analiz_ozeti.txt"
        with open(ozet_dosya_adi, "w", encoding="utf-8") as f:
            f.write(ozet_metni)
        print(f"Özet rapor '{ozet_dosya_adi}' dosyasına da kaydedildi.")
        print(f"--- Haftalık Tarama Bitti. Toplam Süre: {time.time() - start_time_main:.2f} saniye ---")
        return

    print(f"✅ Toplam {len(results_df)} fonun haftalık getirisi hesaplandı.")
    print(f"\n[ADIM 4/5] Fonlar filtreleniyor (Toplam Getiri >= 2%)...")
    filtrelenmis_df = results_df[results_df['Toplam_Getiri'] >= 2].copy()
    
    # Debug sütunları ekle
    filtrelenmis_df['_DEBUG_WeeklyChanges_RAW'] = filtrelenmis_df['Toplam_Getiri']
    filtrelenmis_df['_DEBUG_IsDesiredTrend'] = filtrelenmis_df['Toplam_Getiri'] >= 2
    
    print(f"Filtreleme sonrası {len(filtrelenmis_df)} fon kaldı.")
    if filtrelenmis_df.empty:
        print("⚠️ UYARI: Filtreyi geçen hiçbir fon bulunamadı. 'haftalık' sayfası boş bırakılacak.")
    else:
        print("✅ Filtreleme başarılı. İlk 5 filtrelenmiş fonun başlığı:")
        print(filtrelenmis_df[['Fon Kodu', 'Toplam_Getiri', '_DEBUG_WeeklyChanges_RAW', '_DEBUG_IsDesiredTrend']].head())

    filtrelenmis_df.sort_values(by='Toplam_Getiri', ascending=False, inplace=True)
    
    print(f"\n[ADIM 5/5] Sonuçlar Google Sheets'teki '{WORKSHEET_NAME_WEEKLY}' sayfasına yazılıyor...")

    try:
        spreadsheet = gc.open_by_key(SHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME_WEEKLY)
        worksheet.clear()
        print(f"ℹ️ '{WORKSHEET_NAME_WEEKLY}' sayfası temizlendi.")
        
        # Sadece istenen sütunları yaz
        output_columns = ['Fon Kodu', 'Fon Adı', 'Hafta_1_Getiri', 'Hafta_2_Getiri', 'Toplam_Getiri', '_DEBUG_WeeklyChanges_RAW', '_DEBUG_IsDesiredTrend']
        df_to_write = filtrelenmis_df[output_columns]
        
        worksheet.update([df_to_write.columns.values.tolist()] + df_to_write.values.tolist())
        
        body_resize = {"requests": [{"autoResizeDimensions": {"dimensions": {"sheetId": worksheet.id, "dimension": "COLUMNS"}}}}]}
        spreadsheet.batch_update(body_resize)
        print("✅ Google Sheets 'haftalık' sayfası başarıyla güncellendi.")
    except Exception as e:
        print(f"❌ Google Sheets'e yazma hatası: {e}. 'haftalık' sayfası güncellenemedi.")

    print(f"--- Haftalık Tarama Bitti. Toplam Süre: {time.time() - start_time_main:.2f} saniye ---")

    # Özet Raporu
    toplam_fon_sayisi = len(all_fon_codes)
    basarili_fon_sayisi = len(successful_funds_data)
    nihai_basarisiz_fonlar = current_fon_codes_to_process # Son aşamada kalan başarısızlar
    
    ozet_metni = f"""
--- Analiz Özet Raporu ({datetime.now(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')}) ---
Toplam Analize Alınan Fon Sayısı: {toplam_fon_sayisi}
Başarıyla Analiz Edilen Fon Sayısı: {basarili_fon_sayisi}
Nihai Başarısız Fon Sayısı: {len(nihai_basarisiz_fonlar)}

Aşamalara Göre Başarısız Olan Fonlar:
"""
    for stage_name, failed_funds in failed_funds_by_stage.items():
        ozet_metni += f"- {stage_name}: {len(failed_funds)} fon başarısız oldu.\n"
        if failed_funds:
            ozet_metni += "  Detaylar:\n"
            for fon_kodu, sebep in failed_funds.items():
                ozet_metni += f"  - {fon_kodu}: {sebep}\n"
        else:
            ozet_metni += "  Tüm fonlar bu aşamada başarılı oldu.\n"

    ozet_metni += """
Olası Genel Hata Nedenleri:
- TEFAS web sitesi, kısa sürede yapılan çok sayıda isteği engellemek için 'Rate Limiting' (erişim kısıtlaması) uygulamaktadır.
- Veri çekme işlemi sırasında internet bağlantısı kaynaklı zaman aşımları ('Timeout') yaşanmış olabilir.
- Listede yer alan bazı fon kodları güncel olmayabilir veya TEFAS platformunda bulunamayabilir.
"""
    print(ozet_metni)
    ozet_dosya_adi = "analiz_ozeti.txt"
    with open(ozet_dosya_adi, "w", encoding="utf-8") as f:
        f.write(ozet_metni)

# --- ANA ÇALIŞTIRMA BLOĞU ---
if __name__ == "__main__":
    print("OtoFon Haftalık İvmelenme Taraması Başlatıldı.")
    
    gc_auth = google_sheets_auth()
    if not gc_auth:
        sys.exit(1)

    run_acceleration_scan_and_write_to_sheet(gc_auth, num_weeks=NUM_WEEKS_TO_SCAN)

    print("\n--- Tüm işlemler tamamlandı ---")