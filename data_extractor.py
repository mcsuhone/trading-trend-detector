import pandas as pd
import os
from datetime import datetime

def extract_stocks(input_file='data/debs2022-gc-trading-day-08-11-21.csv', 
                  output_file='data/extracted_stocks.csv',
                  num_stocks=5):
    """
    Extract data for a specified number of stocks from the input CSV file.
    Selects columns by their index positions:
    0: Symbol
    1: SecType
    3: Last
    4: Last volume
    6: Trading time
    7: Total volume
    8: Mid price
    """
    print(f"Starting data extraction from {input_file}")
    
    # Read the CSV file in chunks to handle large file size
    chunk_size = 100000
    selected_stocks = set()
    chunks_to_concat = []
    
    # Specify the columns we want to keep by their index positions
    columns_to_keep = [0, 1, 3, 4, 6, 7, 8]  # Corresponding to the required fields
    column_names = ['Symbol', 'SecType', 'Last', 'Last volume', 
                   'Trading time', 'Total volume', 'Mid price']
    
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
        # If we haven't selected enough stocks yet, get new unique symbols
        if len(selected_stocks) < num_stocks:
            new_stocks = set(chunk['Symbol'].unique())
            selected_stocks.update(list(new_stocks)[:num_stocks - len(selected_stocks)])
        
        # Filter the chunk to only include selected stocks
        filtered_chunk = chunk[chunk['Symbol'].isin(selected_stocks)]
        if not filtered_chunk.empty:
            chunks_to_concat.append(filtered_chunk)
    
    # Combine all filtered chunks
    print(f"Selected stocks: {sorted(selected_stocks)}")
    result_df = pd.concat(chunks_to_concat, ignore_index=True)
    
    # Sort the data by symbol and timestamp
    result_df = result_df.sort_values(['Symbol', 'Trading time'])
    
    # Save to CSV
    result_df.to_csv(output_file, index=False)
    print(f"Data extracted successfully to {output_file}")
    print(f"Total rows in output: {len(result_df)}")

if __name__ == "__main__":
    # Extract data for 5 stocks
    extract_stocks() 