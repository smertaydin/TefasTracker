import os
import pandas as pd
import numpy as np 
from tefas import Crawler 
from supabase import create_client
from datetime import datetime
from pandas.tseries.offsets import BDay

# 1. Setup Supabase
url = os.environ.get("SUPABASE_URL") 
key = os.environ.get("SUPABASE_KEY")

if not url or not key:
    raise ValueError("Supabase keys are missing! Check GitHub Secrets.")

supabase = create_client(url, key)

def update_database():
    print("Fetching data from TEFAS...")
    
    crawler = Crawler()
    end_date = datetime.today()
    start_date = end_date - BDay(1)
    
    end_str = end_date.strftime('%Y-%m-%d')
    start_str = start_date.strftime('%Y-%m-%d')
    
    # --- CHANGE START: Fetch Multiple Kinds ---
    fund_kinds = ['YAT', 'EMK', 'BYF'] # Investment, Pension, ETF
    data_frames = []
    
    for kind in fund_kinds:
        print(f"Fetching {kind} funds...")
        try:
            # Fetch specific kind
            df = crawler.fetch(
                start=start_str, 
                end=end_str, 
                columns=["code", "title", "date", "price"], 
                kind=kind
            )
            data_frames.append(df)
        except Exception as e:
            print(f"Warning: Could not fetch {kind}. Error: {e}")
            
    # Combine all results into one big DataFrame
    if not data_frames:
        print("No data fetched from any source.")
        return

    data = pd.concat(data_frames, ignore_index=True)
    print(f"Total raw rows fetched: {len(data)}")
    # --- CHANGE END ---------------------------
    
    # 2. Pivot & Calculate
    data['date'] = pd.to_datetime(data['date'])
    unique_dates = sorted(data['date'].unique())
    
    if len(unique_dates) < 2:
        print("Not enough data to calculate changes.")
        return

    prev_date = unique_dates[0]
    curr_date = unique_dates[1]
    
    print(f"Calculating change between {prev_date.date()} and {curr_date.date()}")

    # Pivot
    pivot_df = data.pivot_table(index=['code', 'title'], columns='date', values='price')
    pivot_df = pivot_df.dropna() 
    
    # Calculate Return
    pivot_df['daily_return'] = ((pivot_df[curr_date] - pivot_df[prev_date]) / pivot_df[prev_date]) * 100
    
    # Sanitize Data
    pivot_df['daily_return'] = pivot_df['daily_return'].replace([np.inf, -np.inf], 0)
    pivot_df['daily_return'] = pivot_df['daily_return'].fillna(0)

    funds_to_upsert = []
    
    for (code, title), row in pivot_df.iterrows():
        # Safety Casts
        price_val = float(row[curr_date])
        return_val = float(row['daily_return'])
        
        if pd.isna(return_val): return_val = 0.0
        
        funds_to_upsert.append({
            "code": code,
            "name": title,
            "price": price_val,
            "daily_return": return_val, 
            "last_updated": datetime.now().strftime('%Y-%m-%d')
        })
        
    print(f"Prepared {len(funds_to_upsert)} total funds for update.")

    # 3. Batch Upload
    chunk_size = 100
    for i in range(0, len(funds_to_upsert), chunk_size):
        chunk = funds_to_upsert[i:i + chunk_size]
        try:
            response = supabase.table('fund_prices').upsert(chunk).execute()
            print(f"Uploaded batch {i} - {i+len(chunk)}")
        except Exception as e:
            print(f"Batch {i} failed! Retrying row-by-row...")
            for item in chunk:
                try:
                    supabase.table('fund_prices').upsert(item).execute()
                except Exception:
                    pass # Skip bad rows

    print("Database sync complete!")

if __name__ == "__main__":
    update_database()