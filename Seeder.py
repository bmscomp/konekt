import os
import zipfile
import pandas as pd
from pymongo import MongoClient
from kaggle.api.kaggle_api_extended import KaggleApi

def setup_kaggle():
    """Authenticate with Kaggle API and download dataset"""
    # Ensure you have kaggle.json in ~/.kaggle/
    api = KaggleApi()
    api.authenticate()
    
    # Download Wikipedia dataset (example: 'jiashenliu/wikipedia-structure')
    dataset = 'jiashenliu/wikipedia-structure'
    api.dataset_download_files(dataset, path='./data', unzip=True)
    
    print("Dataset downloaded successfully")

def process_csv_to_mongodb():
    """Process CSV files and insert into MongoDB"""
    # Connect to local MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['wikipedia_db']
    
    # Process each CSV file in the downloaded data directory
    data_dir = './data'
    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            collection_name = os.path.splitext(filename)[0]
            
            # Read CSV in chunks for large files
            chunk_size = 10000
            for chunk in pd.read_csv(os.path.join(data_dir, filename), chunksize=chunk_size):
                # Convert DataFrame to dictionary records
                records = chunk.to_dict('records')
                
                # Insert into MongoDB collection
                if records:
                    db[collection_name].insert_many(records)
                    print(f"Inserted {len(records)} records into {collection_name}")
    
    print("All data processed and inserted into MongoDB")

def main():
    # Step 1: Download dataset from Kaggle
    setup_kaggle()
    
    # Step 2: Process and insert into MongoDB
    process_csv_to_mongodb()

if __name__ == '__main__':
    main()
