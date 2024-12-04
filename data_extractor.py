import pandas as pd
import os
from datetime import datetime

# Define the target stocks
TARGET_STOCKS = {'A1EX2F.ETR', 'A2GS63.ETR', 'ALORA.FR', 'IJPHG.FR', 'MLECE.FR'}

def extract_stocks(input_file='data/debs2022-gc-trading-day-08-11-21.csv', 
                  output_file='data/extracted_stocks.csv'):
    """
    Extract data for specific stocks from the input CSV file.
    Selects columns by their index positions:
    0: ID 
    1: SecType
    21: Last
    23: Trading time
    26: Trading date
    """
    print(f"Starting data extraction from {input_file}")
    
    # Read the CSV file in chunks to handle large file size
    chunk_size = 100000
    chunks_to_concat = []
    
    # Specify the columns we want to keep by their index positions
    columns_to_keep = [0, 1, 21, 23, 26]  # Corresponding to the required fields
    column_names = ['ID', 'SecType', 'Last', 'Trading time', 'Trading date']
    
    # Create a directory for the output file if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Process the file in chunks
    for chunk in pd.read_csv(input_file, 
                           chunksize=chunk_size,
                           usecols=columns_to_keep,
                           names=column_names,
                           skiprows=9,  # Skip the metadata rows
                           comment='#'  # Skip any additional comment lines
                           ):
        # Filter the chunk to only include target stocks
        filtered_chunk = chunk[chunk['ID'].isin(TARGET_STOCKS)]
        if not filtered_chunk.empty:
            chunks_to_concat.append(filtered_chunk)
    
    if not chunks_to_concat:
        print("No data found for target stocks!")
        return
    
    # Combine all filtered chunks
    print(f"Processing data for stocks: {sorted(TARGET_STOCKS)}")
    result_df = pd.concat(chunks_to_concat, ignore_index=True)
    
    # Sort the data by ID and timestamp
    result_df = result_df.sort_values(['ID', 'Trading time'])
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"\nExtraction Statistics:")
    print(f"Total rows extracted: {len(result_df)}")
    print(f"\nRows per stock:")
    print(result_df['ID'].value_counts().to_string())
    print(f"\nSample of extracted data:")
    print(result_df.head())
    print(f"\nData extracted successfully to {output_file}")

if __name__ == "__main__":
    extract_stocks() 