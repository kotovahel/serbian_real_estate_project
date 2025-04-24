import time

import pandas as pd
import requests
import os
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows


def reverse_geocode(lat, lon, server_url="http://localhost:8080", language="sr-Latn"):
    """
    Query the Nominatim server for detailed address components.
    """
    endpoint = f"{server_url}/reverse"
    params = {
        "lat": lat,
        "lon": lon,
        "format": "json",
        "zoom": 18,
        "addressdetails": 1,
        "accept-language": language
    }
    
    try:
        response = requests.get(endpoint, params=params, timeout=5)
        response.raise_for_status()
        response.encoding = 'utf-8'
        data = response.json()
        return {
            "display_name": data.get("display_name", "Unknown address"),
            "address": data.get("address", {})
        }
    except requests.exceptions.RequestException as e:
        print(f"Error for ({lat}, {lon}): {e}")
        return {"display_name": "Error", "address": {}}


def process_row(row, lat_col, lon_col, server_url):
    """Helper function to process a single row."""
    lat = row[lat_col]
    lon = row[lon_col]
    return (lat, lon, reverse_geocode(lat, lon, server_url))


def parse_address(address_data):
    """
    Parse Nominatim’s detailed address components into OpenStreetMap-style columns.
    """
    display_name = address_data["display_name"]
    address = address_data["address"]
    
    if display_name == "Error" or not address:
        return {
            "house_number": None,
            "road": None,
            "village": None,
            "municipality": None,
            "county": None,
            "state": None,
            "postcode": None,
            "country": None
        }
    
    parsed = {
        "house_number": address.get("house_number"),
        "road": address.get("road"),
        "village": address.get("village") or address.get("town"),
        "municipality": (address.get("city") if address.get("city") and ("Opština" in address.get("city") or "Grad" in address.get("city")) 
                         else address.get("municipality") or address.get("suburb")),
        "county": address.get("county"),
        "state": address.get("state"),
        "postcode": address.get("postcode"),
        "country": address.get("country", "Srbija")
    }
    
    if not parsed["municipality"] and "Opština" in display_name:
        parts = [part.strip() for part in display_name.split(",")]
        for part in parts:
            if "Opština" in part:
                parsed["municipality"] = part
                break
    
    return parsed


def process_csv(input_file, output_file, server_url="http://localhost:8080", max_workers=4):
    """
    Read a CSV, reverse geocode coordinates, parse addresses, and save to XLSX with minimal formatting.
    """
    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(input_file, encoding='utf-8')
    
    # Ensure latitude and longitude columns exist (case-insensitive)
    cols = {col.lower(): col for col in df.columns}
    lat_col = cols.get("latitude")
    lon_col = cols.get("longitude")
    
    if not lat_col or not lon_col:
        print(f"Skipping {input_file}: CSV must contain 'latitude' and 'longitude' columns")
        return
    
    # Initialize columns with Nominatim-style names
    df["display_name"] = None
    df["house_number"] = None
    df["road"] = None
    df["village"] = None
    df["municipality"] = None
    df["county"] = None
    df["state"] = None
    df["postcode"] = None
    df["country"] = None
    
    # Filter out rows with NaN or invalid lat/lon values
    valid_rows = df.dropna(subset=[lat_col, lon_col])
    valid_rows = valid_rows[valid_rows[lat_col].apply(pd.to_numeric, errors='coerce').notna()]
    valid_rows = valid_rows[valid_rows[lon_col].apply(pd.to_numeric, errors='coerce').notna()]
    
    # Process only valid rows in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_coords = {
            executor.submit(process_row, row, lat_col, lon_col, server_url): index
            for index, row in valid_rows.iterrows()
        }
        for future in as_completed(future_to_coords):
            index = future_to_coords[future]
            try:
                lat, lon, address_data = future.result()
                df.at[index, "display_name"] = address_data["display_name"]
                parsed = parse_address(address_data)
                for key, value in parsed.items():
                    df.at[index, key] = value
                print(f"Processed row {index} in {os.path.basename(input_file)}: ({lat}, {lon})")
            except Exception as e:
                print(f"Error at index {index} in {os.path.basename(input_file)}: {e}")
                df.at[index, "display_name"] = "Error"
    
    # Calculate error statistics
    total_rows = len(df)
    error_count = len(df[df["display_name"] == "Error"])
    error_percentage = (error_count / total_rows * 100) if total_rows > 0 else 0
    
    # Print error statistics
    error_msg = f"{os.path.basename(input_file)}: Errors: {error_count}/{total_rows} ({error_percentage:.2f}%)"
    print(error_msg)
    logging.info(error_msg)
    
    # Save to XLSX with minimal formatting
    wb = Workbook()
    ws = wb.active
    for r in dataframe_to_rows(df, index=False, header=True):
        ws.append(r)
    
    wb.save(output_file)
    print(f"Results saved to {output_file}")


def process_folder(input_folder, output_folder, server_url="http://localhost:8080", max_workers=4):
    """
    Process all CSV files in a folder and save results to an output folder.
    """

    # server_url = 'https://nominatim.openstreetmap.org'
    # Ensure input folder exists
    if not os.path.isdir(input_folder):
        raise ValueError(f"{input_folder} is not a valid directory")

    # Create output folder and log subfolder
    os.makedirs(output_folder, exist_ok=True)
    log_folder = os.path.join(output_folder, "log")
    os.makedirs(log_folder, exist_ok=True)

    # Set up logging
    log_file = os.path.join(log_folder, f"geocode_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(message)s'
    )

    # Find all CSV files in the folder
    csv_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.csv')]

    if not csv_files:
        print(f"No CSV files found in {input_folder}")
        logging.info(f"No CSV files found in {input_folder}")
        return

    # Process each CSV file
    created_file_counter = len(csv_files)
    for csv_file in csv_files:
        input_path = os.path.join(input_folder, csv_file)
        output_filename = os.path.splitext(csv_file)[0] + "_with_location.xlsx"
        if output_filename in os.listdir(output_folder):
            created_file_counter -= 1
            continue
        output_path = os.path.join(output_folder, output_filename)

        print(f"Processing {input_path}...")
        logging.info(f"Processing {input_path}")
        process_csv(input_path, output_path, server_url, max_workers)
    print(f'Created files .xlsx: {created_file_counter}')
    return created_file_counter


# Example usage
if __name__ == "__main__":
    input_folder = r"D:\Users\bojan.jevtic.ed\Desktop\test"
    output_folder = r"D:\Users\bojan.jevtic.ed\Desktop\test\output"
    process_folder(input_folder, output_folder, max_workers=2)