import argparse
import os
import hashlib
import time
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
import multiprocessing as mp
from io import StringIO
import sys


def print_progress_bar(iteration: int, total: int, bar_length: int = 40) -> None:
    """
    Print a textual progress bar to the console.

    :param iteration: Current iteration or progress value.
    :param total: Total number of iterations or progress end value.
    :param bar_length: Length of the progress bar in characters. Defaults to 40.
    :rtype: None
    """
    
    # Calculate the percentage of completion
    percent_complete = (iteration / total)
    
    # Calculate the number of characters to show in the progress bar
    num_hashes = int(round(percent_complete * bar_length))
    
    # Create the progress bar string
    bar = '#' * num_hashes + '-' * (bar_length - num_hashes)
    
    # Print the progress bar
    sys.stdout.write(f'\r[{bar}] {iteration}/{total} ({percent_complete:.1%})')
    sys.stdout.flush()


def get_file_hash(file_path: str) -> str:
    """
    Calculate the MD5 hash of a file.

    :param file_path: Path to the file to be hashed.
    :return: The hexadecimal digest of the file hash.
    :rtype: str
    """
    
    # Create a hash object using MD5
    hash_obj = hashlib.md5()
    
    # Read the file in binary mode
    with open(file_path, 'rb') as file:
        # Read and update the hash object in chunks
        while chunk := file.read(8192):  # You can adjust the chunk size
            hash_obj.update(chunk)
    
    # Return the hexadecimal representation of the digest
    return hash_obj.hexdigest()


def is_valid_data(chunk: pd.DataFrame) -> pd.Series:
    """
    Validate the data in each chunk.

    :param chunk: DataFrame containing the data to validate.
    :return: Boolean Series indicating which rows are valid.
    :rtype: pd.Series
    """
    
    # Validate each row individually and return a boolean series
    valid = pd.Series(True, index=chunk.index)
    
    # Check for the expected columns
    if not all(col in chunk.columns for col in ['date', 'time', 'consumption']):
        valid[:] = False
    
    # Check if 'consumption' values are numeric
    valid &= pd.to_numeric(chunk['consumption'], errors='coerce').notnull()
    
    # Ensure 'date' and 'time' are correctly parsed
    valid &= ~chunk['date'].isnull() & ~chunk['time'].isnull()
    
    return valid


def process_csv_file(file_path: str) -> pd.DataFrame:
    """
    Process a CSV file, validate its content, and aggregate the data.

    :param file_path: Path to the CSV file to process.
    :return: Aggregated DataFrame of valid data or None if no valid data.
    :rtype: pd.DataFrame
    """
    
    try:
        chunk_size = 10000  # Adjust this based on your memory constraints
        chunks = []
        
        for chunk in pd.read_csv(
            file_path, 
            skiprows=12, 
            header=None, 
            names=['date', 'time', 'consumption'], 
            dayfirst=True, 
            chunksize=chunk_size
        ):
            # Convert the 'date' and 'time' columns to appropriate types
            chunk['date'] = pd.to_datetime(chunk['date'], dayfirst=True, errors='coerce').dt.date
            chunk['time'] = pd.to_datetime(chunk['time'], format='%H:%M', errors='coerce').dt.floor('H').dt.time
            
            invalid_rows = ~is_valid_data(chunk)
            
            if invalid_rows.any():
                print(f"Invalid data in {invalid_rows.sum()} rows in file {file_path}. Discarding these rows.")
                chunk = chunk[~invalid_rows]  # Keep only valid rows
            
            # If all rows are invalid, skip to the next chunk
            if chunk.empty:
                continue
            
            # Group by 'date' and 'time' and aggregate the consumption data
            chunk = chunk.groupby(['date', 'time'])['consumption'].agg(['sum', 'count']).reset_index()
            chunk['count'] = 1
            chunks.append(chunk)
        
        # Concatenate all valid chunks into a single DataFrame
        if chunks:
            return pd.concat(chunks, ignore_index=True)
        else:
            return None
    
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


def process_csv_files(directory: str) -> pd.DataFrame:
    """
    Process all CSV files in a directory, filter out duplicates, and aggregate the data.

    :param directory: Path to the directory containing CSV files.
    :return: Aggregated DataFrame of valid data from all files.
    :rtype: pd.DataFrame
    """
    
    processed_files = set()
    all_data = []

    file_paths = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]
    total_files = len(file_paths)
    
    with mp.Pool(processes=mp.cpu_count()) as pool:
        # Calculate file hashes in parallel
        file_hashes = pool.starmap(get_file_hash, [(f,) for f in file_paths])
        
        # Filter out duplicate files
        unique_files = [
            f for f, h in zip(file_paths, file_hashes) 
            if h not in processed_files and not processed_files.add(h)
        ]
        
        # Process unique files with progress bar
        results = []
        for i, df in enumerate(pool.imap(process_csv_file, unique_files), 1):
            print_progress_bar(i, total_files)
            if df is not None and not df.empty:
                all_data.append(df)
        
        # Print progress bar at 100% completion
        print_progress_bar(total_files, total_files)
        print()  # Move to the next line after the progress bar

    return (
        pd.concat(all_data)
        .groupby(['date', 'time'])
        .agg({'sum': 'sum', 'count': 'sum'})
        .reset_index()
    )


def create_and_insert_into_postgres(data: pd.DataFrame) -> None:
    """
    Create a PostgreSQL table and insert aggregated data.

    :param data: DataFrame containing the aggregated data to insert.
    :rtype: None
    """
    
    conn = psycopg2.connect(
        dbname="dummydb",
        user="user",
        password="password",
        host="localhost",
        port="5432"
    )
    
    cur = conn.cursor()

    # change to aggregate logic if you want to add to existing table
    # ---------------------------------------------------------------------
    # Drop the table if it exists
    cur.execute("DROP TABLE IF EXISTS table_name")
    
    # Create a new table
    cur.execute("""
        CREATE TABLE table_name (
            date DATE,
            time TIME,
            consumption FLOAT,
            count INTEGER,
            PRIMARY KEY (date, time)
        )
    """)
    # ---------------------------------------------------------------------

    # Convert DataFrame to CSV string
    output = StringIO()
    data.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)

    # Use copy_from for bulk insert
    cur.copy_from(output, 'table_name', columns=('date', 'time', 'consumption', 'count'))

    conn.commit()

    # Verify the data in the table
    cur.execute("SELECT COUNT(*) FROM table_name")
    count = cur.fetchone()[0]
    print(f"Total rows in the table: {count}")

    cur.close()
    conn.close()


def main() -> None:
    """
    Main function to process CSV files and insert data into PostgreSQL.

    :rtype: None
    """
    
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="The directory for the file parser")
    
    # Add the directory argument
    parser.add_argument(
        'directory',
        type=str,
        help="Path to the directory containing CSV files."
    )
    
    # Parse the command-line arguments
    args = parser.parse_args()
    
    # Get the directory from the arguments
    directory = args.directory
    
    # Process the CSV files in the specified directory
    processed_data = process_csv_files(directory)
    
    if not processed_data.empty:
        create_and_insert_into_postgres(processed_data)
        print(f"Processed {len(processed_data)} unique date-time entries.")
    else:
        print("No valid data to process.")

if __name__ == "__main__":
    main()