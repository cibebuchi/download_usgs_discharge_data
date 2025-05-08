# USGS Streamflow Data Retrieval

**Created by Dr. Chibuike Ibebuchi**

**Files included in this repository:**

- `usgs_data_retrieval.py`
- `requirements.txt`
- `README.md`

## Features

- Fetches site metadata for one or more US states
- Downloads daily discharge data (1970-01-01 to 2024-12-31) #adjust 
- Merges onto a complete daily calendar, filling missing days with `NaN`
- Computes completeness percentage for each station
- Saves one CSV per station in the output directory
- Copies stations with ≥95% completeness to a separate directory
- Generates a summary CSV of completeness percentages

## Requirements

See `requirements.txt`.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
python usgs_data_retrieval.py   --states CA OR WA   --output-dir ./data/final   --complete-dir ./data/complete
```

## Output

- `usgs_sites_metadata.csv`: metadata for all fetched sites
- `STATE_SITEID_StationName.csv`: daily data for each station
- `completeness_summary.csv`: summary of percent completeness


