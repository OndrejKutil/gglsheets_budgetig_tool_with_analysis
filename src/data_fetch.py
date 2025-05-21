"""
Data fetching module for Google Sheets integration.
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import logging
import os
from typing import List, Dict, Any
from functools import wraps
from datetime import datetime, timedelta
import pandas as pd

# Get the absolute path to the token file relative to this script
current_dir = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE_PATH = os.path.join(current_dir, 'tokens', 'new-project-01-449515-0a3860dea29d.json')

# Set up logging
logger = logging.getLogger('data_fetch')
logger.setLevel(logging.INFO)
handler = logging.FileHandler(os.path.join(current_dir, 'data_fetch_debug.log'))
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def get_gspread_client():
    """Get an authenticated gspread client."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(TOKEN_FILE_PATH, scope)
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Failed to get gspread client: {e}")
        raise

def get_transactions(spreadsheet_name: str, worksheet_name: str) -> pd.DataFrame:
    """Fetch transactions from Google Sheet directly with no caching."""
    start_time = time.time()
    logger.info(f"Starting fetch from {spreadsheet_name}/{worksheet_name}")
    
    try:
        # Create a new client for each request to avoid token expiration issues
        client = get_gspread_client()
        logger.info("Successfully authenticated with Google Sheets API")
        
        # Open the spreadsheet and worksheet
        sheet = client.open(spreadsheet_name)
        worksheet = sheet.worksheet(worksheet_name)
        
        # Get all values from the worksheet
        all_values = worksheet.get_all_values()
        logger.info(f"Retrieved {len(all_values)} rows from worksheet")
        
        if not all_values or len(all_values) <= 1:  # Need at least header row and one data row
            logger.warning("No data or only header row found in worksheet")
            return pd.DataFrame()
        
        # First row is headers
        headers = all_values[0]
        data_rows = all_values[1:]
        logger.info(f"Headers: {len(headers)}, : {headers}")
        logger.info(f"Data rows: {len(data_rows)}")
        
        # Convert data rows to DataFrame
        df = pd.DataFrame(data_rows, columns=headers)

        # Clean and convert VALUE field
        if 'VALUE' in df.columns:
            df['VALUE'] = (
            df['VALUE']
            .astype(str)
            .str.strip()
            .replace('', '0')
            .str.replace('Kč', '', regex=False)
            .str.replace(',', '', regex=False)
            .str.replace(' ', '', regex=False)
            )
            df['VALUE_NUMERIC'] = pd.to_numeric(df['VALUE'], errors='coerce').fillna(0.0)
        else:
            df['VALUE'] = '0'
            df['VALUE_NUMERIC'] = 0.0

        # Ensure all expected fields are present
        for field in ['DATE', 'DESCRIPTION', 'CATEGORY', 'ACCOUNT', 'TYPE', 'MONTH']:
            if field not in df.columns:
                df[field] = ""

        elapsed = time.time() - start_time
        return df
        
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error fetching transactions after {elapsed:.2f}s: {e}", exc_info=True)
        return pd.DataFrame()


def get_worksheet(spreadsheet_name: str, worksheet_name: str, 
                 row_start: int, column_start: int, num_columns: int = 0) -> List[Dict[str, Any]]:
    """Unused function, only for data exploration and debugging.
    Allows fetching a specific range of data from a Google Sheet."""
    start_time = time.time()
    logger.info(f"Starting get_worksheet for {spreadsheet_name}/{worksheet_name}")
    
    try:
        # Create a new client for each request
        client = get_gspread_client()
        
        # Open the spreadsheet and worksheet
        sheet = client.open(spreadsheet_name)
        worksheet = sheet.worksheet(worksheet_name)
        all_values = worksheet.get_all_values()
        
        if not all_values or row_start >= len(all_values):
            logger.warning(f"No data or row_start {row_start} beyond data length {len(all_values)}")
            return []
        
        # Validate parameters
        if row_start < 1:
            row_start = 1
            
        if column_start < 1:
            column_start = 1
        
        # Adjust for 0-based indexing
        row_idx = row_start - 1
        col_idx = column_start - 1
        
        # Get headers (adjusting for column start)
        if col_idx >= len(all_values[row_idx]):
            logger.warning(f"Column start {column_start} is beyond the width of row {row_start}")
            return []
            
        headers = all_values[row_idx][col_idx:]
        if num_columns != 0 and num_columns < len(headers):
            headers = headers[:num_columns]
        
        # Process data rows
        result = []
        for data_row_idx in range(row_idx + 1, len(all_values)):
            data_row = all_values[data_row_idx]
            
            # Skip if row is too short
            if col_idx >= len(data_row):
                continue
                
            # Extract the relevant portion of the row
            row_data = data_row[col_idx:]
            if num_columns != 0 and num_columns < len(row_data):
                row_data = row_data[:num_columns]
                
            # Pad row data if needed
            while len(row_data) < len(headers):
                row_data.append("")
                
            # Create dict from headers and row data
            row_dict = dict(zip(headers, row_data))
            result.append(row_dict)
        
        elapsed = time.time() - start_time
        logger.info(f"Retrieved and processed {len(result)} rows in {elapsed:.2f}s")
        return result
        
    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(f"Error in get_worksheet after {elapsed:.2f}s: {e}", exc_info=True)
        # Return empty list instead of raising exception
        return []