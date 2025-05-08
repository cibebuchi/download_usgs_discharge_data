#!/usr/bin/env python3
"""
USGS Streamflow Data Retrieval Script

Downloads daily discharge (parameter 00060) for specified USGS sites,
merges onto a complete daily calendar, computes completeness percentage over a
static window (1970-01-01 to 2024-12-31), and saves CSV outputs.

Usage:
    python usgs_data_retrieval.py --output-dir ./data/final --complete-dir ./data/complete
"""

import os
import shutil
import logging
import argparse
from datetime import datetime
import pandas as pd
import dataretrieval.nwis as nwis
from tqdm import tqdm
import re

# Default USGS parameter code for daily discharge
USGS_PARAM = '00060'
# Default list of western US state codes
DEFAULT_STATES = ['CA','OR','WA','ID','NV','UT','AZ','MT','WY','CO','NM']

# Static calendar window for completeness calculation
START_DATE = '1970-01-01'
END_DATE   = '2024-12-31'
FULL_DATES = pd.date_range(start=START_DATE, end=END_DATE, freq='D')
TOTAL_DAYS = len(FULL_DATES)

def ensure_writable(directory):
    """Ensure the directory exists and is writable."""
    os.makedirs(directory, exist_ok=True)
    test_path = os.path.join(directory, '.write_test')
    with open(test_path, 'w') as f:
        f.write('ok')
    os.remove(test_path)

def get_usgs_sites(state_code):
    """Fetch USGS streamflow site metadata for a given state."""
    try:
        sites, _ = nwis.get_info(
            stateCd     = state_code,
            parameterCd = USGS_PARAM,
            siteTypeCd  = 'ST',
            siteStatus  = 'all'
        )
        return sites
    except Exception as e:
        logging.error(f"Error fetching sites for {state_code}: {e}")
        return pd.DataFrame()

def download_and_process(site, output_dir, complete_dir):
    """Download, process, save, and optionally copy one site's data."""
    site_id = site['site_no']
    name    = site['station_nm']
    state   = site['state_cd']
    lon     = site['dec_long_va']
    lat     = site['dec_lat_va']

    df, _ = nwis.get_dv(
        sites       = site_id,
        parameterCd = USGS_PARAM,
        start       = START_DATE,
        end         = END_DATE
    )
    if df.empty:
        return None

    if hasattr(df.index, 'tz') and df.index.tz:
        df.index = df.index.tz_localize(None)
    df = df.rename_axis('Date').reset_index()

    col = next((c for c in df.columns if USGS_PARAM in c), None)
    if not col:
        return None
    df = df.rename(columns={col: 'discharge_cfs'})[['Date','discharge_cfs']]
    df['Date'] = pd.to_datetime(df['Date'])

    calendar = pd.DataFrame({'Date': FULL_DATES})
    merged   = calendar.merge(df, on='Date', how='left')
    merged['site'], merged['lon'], merged['lat'] = site_id, lon, lat

    count_non_na = merged['discharge_cfs'].notna().sum()
    pct_complete = count_non_na / TOTAL_DAYS * 100

    safe_name = re.sub(r"[^\w\-]", "_", name)
    filename  = f"{state}_{site_id}_{safe_name}.csv"
    out_path  = os.path.join(output_dir, filename)

    merged.to_csv(out_path, index=False)
    print(f"[{site_id}] Saved {filename} ({pct_complete:.1f}% complete)")

    if pct_complete >= 95:
        shutil.copy(out_path, os.path.join(complete_dir, filename))
        print(f"[{site_id}] Copied to complete_dir")

    return {
        'site_no':          site_id,
        'station_nm':       name,
        'dec_long_va':      lon,
        'dec_lat_va':       lat,
        'percent_complete': pct_complete
    }

def main():
    parser = argparse.ArgumentParser(description="USGS Streamflow Data Retrieval")
    parser.add_argument('--states', nargs='+', default=DEFAULT_STATES,
                        help='List of US state codes to fetch sites for')
    parser.add_argument('--output-dir', default='./data/final', help='Directory for CSV outputs')
    parser.add_argument('--complete-dir', default='./data/complete', help='Directory for >=95% complete CSVs')
    args = parser.parse_args()

    ensure_writable(args.output_dir)
    ensure_writable(args.complete_dir)

    metadata_frames = []
    for st in args.states:
        df = get_usgs_sites(st)
        if not df.empty:
            metadata_frames.append(df)
    if not metadata_frames:
        print("No sites found for given states.")
        return

    meta_df = pd.concat(metadata_frames, ignore_index=True)
    meta_df.to_csv(os.path.join(args.output_dir, 'usgs_sites_metadata.csv'), index=False)
    print("Site metadata saved.")

    summary = []
    for _, row in tqdm(meta_df.iterrows(), total=len(meta_df)):
        rec = download_and_process(row, args.output_dir, args.complete_dir)
        if rec:
            summary.append(rec)

    pd.DataFrame(summary).to_csv(
        os.path.join(args.output_dir, 'completeness_summary.csv'),
        index=False
    )
    print("Done! Summary saved.")

if __name__ == '__main__':
    main()
