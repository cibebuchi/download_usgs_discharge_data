#!/usr/bin/env python3
"""
USGS Streamflow Data Retrieval Script

Downloads daily discharge data (default: parameter 00060) for USGS sites in specified states,
merges onto a complete daily calendar, computes completeness percentage over a static window,
and saves CSV outputs. Copies high-completeness CSVs to a separate directory.

Usage:
    python usgs_data_retrieval.py --output-dir ./data/final --complete-dir ./data/complete \
        --states CA OR WA --start-date 1970-01-01 --end-date 2024-12-31 --completeness-threshold 95

Dependencies:
    pandas, dataretrieval, tqdm, requests
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
import requests
import sys
import pkg_resources

# Configure logging
logging.basicConfig(
    filename='usgs_data_retrieval.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.getLogger().addHandler(logging.StreamHandler())

# Default parameters
DEFAULT_STATES = ['CA', 'OR', 'WA', 'ID', 'NV', 'UT', 'AZ', 'MT', 'WY', 'CO', 'NM']
DEFAULT_PARAM = '00060'  # Daily discharge
DEFAULT_START = '1970-01-01'
DEFAULT_END = '2024-12-31'

def validate_date(date_str):
    """Validate date string format (YYYY-MM-DD)."""
    try:
        return pd.to_datetime(date_str)
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")

def ensure_writable(directory):
    """Ensure the directory exists and is writable."""
    try:
        os.makedirs(directory, exist_ok=True)
        test_path = os.path.join(directory, '.write_test')
        with open(test_path, 'w') as f:
            f.write('ok')
        os.remove(test_path)
    except (OSError, PermissionError) as e:
        logging.error(f"Cannot create/write to directory {directory}: {e}")
        raise

def check_dependencies():
    """Check if required packages are installed."""
    required = ['pandas', 'dataretrieval', 'tqdm', 'requests']
    missing = []
    for pkg in required:
        try:
            pkg_resources.get_distribution(pkg)
        except pkg_resources.DistributionNotFound:
            missing.append(pkg)
    if missing:
        logging.error(f"Missing dependencies: {', '.join(missing)}")
        raise ImportError(f"Please install missing packages: pip install {' '.join(missing)}")

def get_usgs_sites(state_code):
    """Fetch USGS streamflow site metadata for a given state."""
    try:
        sites, _ = nwis.get_info(
            stateCd=state_code,
            parameterCd=DEFAULT_PARAM,
            siteTypeCd='ST',
            siteStatus='all'
        )
        if sites.empty:
            logging.warning(f"No sites found for state {state_code}")
        return sites
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error fetching sites for {state_code}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error fetching sites for {state_code}: {e}")
        return pd.DataFrame()

def download_and_process(site, output_dir, complete_dir, start_date, end_date, completeness_threshold):
    """Download, process, save, and optionally copy one site's data."""
    try:
        site_id = site['site_no']
        name = site['station_nm']
        state = site['state_cd']
        lon = site['dec_long_va']
        lat = site['dec_lat_va']

        df, _ = nwis.get_dv(
            sites=site_id,
            parameterCd=DEFAULT_PARAM,
            start=start_date,
            end=end_date
        )
        if df.empty:
            logging.warning(f"No data for site {site_id}")
            return None

        # Remove timezone if present
        if hasattr(df.index, 'tz') and df.index.tz:
            df.index = df.index.tz_localize(None)
        df = df.rename_axis('Date').reset_index()

        # Find discharge column
        col = next((c for c in df.columns if DEFAULT_PARAM in c), None)
        if not col:
            logging.warning(f"No discharge column for site {site_id}")
            return None
        df = df.rename(columns={col: 'discharge_cfs'})[['Date', 'discharge_cfs']]
        df['Date'] = pd.to_datetime(df['Date'])

        # Create calendar and merge
        full_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        calendar = pd.DataFrame({'Date': full_dates})
        merged = calendar.merge(df, on='Date', how='left')
        merged['site'], merged['lon'], merged['lat'] = site_id, lon, lat

        # Compute completeness
        count_non_na = merged['discharge_cfs'].notna().sum()
        total_days = len(full_dates)
        pct_complete = (count_non_na / total_days * 100) if total_days > 0 else 0

        # Save CSV
        safe_name = re.sub(r"[^\w\-]", "_", name)
        filename = f"{state}_{site_id}_{safe_name}.csv"
        out_path = os.path.join(output_dir, filename)
        merged.to_csv(out_path, index=False)
        logging.info(f"[{site_id}] Saved {filename} ({pct_complete:.1f}% complete)")
        print(f"[{site_id}] Saved {filename} ({pct_complete:.1f}% complete)")

        # Copy to complete_dir if above threshold
        if pct_complete >= completeness_threshold:
            shutil.copy(out_path, os.path.join(complete_dir, filename))
            logging.info(f"[{site_id}] Copied to complete_dir")
            print(f"[{site_id}] Copied to complete_dir")

        return {
            'site_no': site_id,
            'station_nm': name,
            'dec_long_va': lon,
            'dec_lat_va': lat,
            'percent_complete': pct_complete
        }
    except Exception as e:
        logging.error(f"Error processing site {site.get('site_no', 'unknown')}: {e}")
        return None

def main():
    """Main function to retrieve and process USGS streamflow data."""
    parser = argparse.ArgumentParser(description="USGS Streamflow Data Retrieval")
    parser.add_argument('--states', nargs='+', default=DEFAULT_STATES,
                        help=f'List of US state codes (default: {DEFAULT_STATES})')
    parser.add_argument('--output-dir', default='./data/final',
                        help='Directory for CSV outputs (default: ./data/final)')
    parser.add_argument('--complete-dir', default='./data/complete',
                        help='Directory for high-completeness CSVs (default: ./data/complete)')
    parser.add_argument('--start-date', default=DEFAULT_START,
                        help=f'Start date (YYYY-MM-DD, default: {DEFAULT_START})')
    parser.add_argument('--end-date', default=DEFAULT_END,
                        help=f'End date (YYYY-MM-DD, default: {DEFAULT_END})')
    parser.add_argument('--completeness-threshold', type=float, default=95,
                        help='Completeness threshold for copying CSVs (default: 95)')
    args = parser.parse_args()

    # Validate inputs
    try:
        start_date = validate_date(args.start_date)
        end_date = validate_date(args.end_date)
        if start_date >= end_date:
            raise ValueError("start_date must be before end_date")
        if args.completeness_threshold < 0 or args.completeness_threshold > 100:
            raise ValueError("completeness_threshold must be between 0 and 100")
        valid_states = [st.upper() for st in args.states]
        for st in valid_states:
            if not re.match(r'^[A-Z]{2}$', st):
                raise ValueError(f"Invalid state code: {st}")
    except ValueError as e:
        logging.error(f"Input validation error: {e}")
        print(f"Error: {e}")
        return

    # Check dependencies
    try:
        check_dependencies()
    except ImportError as e:
        print(e)
        return

    # Ensure directories are writable
    try:
        ensure_writable(args.output_dir)
        ensure_writable(args.complete_dir)
    except (OSError, PermissionError) as e:
        print(f"Directory error: {e}")
        return

    # Fetch site metadata
    metadata_frames = []
    for st in valid_states:
        logging.info(f"Fetching sites for state {st}")
        df = get_usgs_sites(st)
        if not df.empty:
            metadata_frames.append(df)
    if not metadata_frames:
        logging.error("No sites found for given states.")
        print("No sites found for given states.")
        return

    meta_df = pd.concat(metadata_frames, ignore_index=True)
    meta_path = os.path.join(args.output_dir, 'usgs_sites_metadata.csv')
    meta_df.to_csv(meta_path, index=False)
    logging.info(f"Site metadata saved to {meta_path}")
    print(f"Site metadata saved to {meta_path}")

    # Process sites
    summary = []
    for _, row in tqdm(meta_df.iterrows(), total=len(meta_df), desc="Processing sites"):
        rec = download_and_process(row, args.output_dir, args.complete_dir,
                                  args.start_date, args.end_date, args.completeness_threshold)
        if rec:
            summary.append(rec)

    # Save summary
    if summary:
        summary_df = pd.DataFrame(summary)
        summary_path = os.path.join(args.output_dir, 'completeness_summary.csv')
        summary_df.to_csv(summary_path, index=False)
        logging.info(f"Summary saved to {summary_path}")
        print(f"Done! Summary saved to {summary_path}")
    else:
        logging.warning("No data processed successfully.")
        print("No data processed successfully.")

if __name__ == '__main__':
    main()
