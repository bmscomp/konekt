import os
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from kaggle.api.kaggle_api_extended import KaggleApi

def setup_kaggle():
    """Authenticate with Kaggle API and download dataset"""
    # Ensure you have kaggle.json in ~/.kaggle/
    api = KaggleApi()
    api.authenticate()
    
    # Download Wikipedia dataset (example: 'wikimedia-foundation/wikipedia-structured-contents/data')
    dataset = 'wikimedia-foundation/wikipedia-structured-contents/data'
    api.dataset_download_files(dataset, path='./data', unzip=True)
    
    print("Dataset downloaded successfully")

def process_json_to_mongodb():

    load_dotenv()  # Load from .env file

    """Process JSON files and insert into MongoDB"""
    # Connect to local MongoDB
    client = MongoClient(
        host='mongodb://localhost:27017/',
        username=os.getenv('MONGO_USERNAME'),  # or replace with actual username
        password=os.getenv('MONGO_PASSWORD'),  # or replace with actual password
        authSource='admin',  # The authentication database (usually 'admin')
        authMechanism='SCRAM-SHA-256'  # Default modern auth mechanism
    )

    db = client['wikipedia']  # Use or create a database named 'wikipedia'
    
    # Process each JSON file in the downloaded data directory
    data_dir = './data'
    for filename in os.listdir(data_dir):
        collection_name = 'pages'
        if filename.endswith('.json'):
            # Read JSON file
            file_path = os.path.join(data_dir, filename)
            print(f"Processing {file_path}...")
            
            try:
                # For large files, we'll process in batches
                with open(file_path, 'r', encoding='utf-8') as file:
                    # Check if the file contains a JSON array or a JSON object per line
                    first_char = file.read(1)
                    file.seek(0)  # Reset file pointer
                    
                    if first_char == '[':
                        # File contains a JSON array
                        data = json.load(file)
                        
                        # Process in batches of 1000 records
                        batch_size = 100000
                        for i in range(0, len(data), batch_size):
                            batch = data[i:i+batch_size]
                            if batch:
                                db[collection_name].insert_many(batch)
                                print(f"Inserted {len(batch)} records into {collection_name}")
                    else:
                        # File contains JSON objects, one per line (JSON Lines format)
                        batch = []
                        for line_number, line in enumerate(file):
                            try:
                                record = json.loads(line.strip())
                                batch.append(record)
                                
                                # Insert in batches of 10000
                                if len(batch) >= 100000:
                                    db[collection_name].insert_many(batch)
                                    print(f"Inserted {len(batch)} records into {collection_name}")
                                    batch = []
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON on line {line_number+1}: {e}")
                        
                        # Insert any remaining records
                        if batch:
                            db[collection_name].insert_many(batch)
                            print(f"Inserted {len(batch)} records into {collection_name}")
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    
    print("All data processed and inserted into MongoDB")

def main():
    
    # Step 2: Process and insert into MongoDB
    process_json_to_mongodb()

if __name__ == '__main__':
    main()
