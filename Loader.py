import os
import json
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from tqdm import tqdm

def load_jsonl_files_to_mongodb(data_directory, db, collection):
    """
    Load all .jsonl files from a directory into MongoDB collection
    
    Args:
        data_directory: Path to directory containing .jsonl files
        db: MongoDB database name
        collection: MongoDB collection name
    """

    client = MongoClient("mongodb://root:rootpassword@localhost:27017/?replicaSet=rs0&authSource=admin&directConnection=true")
    db = client[db]
    collection = db[collection]
    
    # Get all .jsonl files in directory
    jsonl_files = [
        f for f in os.listdir(data_directory) 
        if f.endswith('.jsonll') and os.path.isfile(os.path.join(data_directory, f))
    ]
    
    if not jsonl_files:
        print(f"No .jsonl files found in directory: {data_directory}")
        return
    
    print(f"Found {len(jsonl_files)} .jsonl files to process")
    
    # Process each file
    for filename in jsonl_files:
        filepath = os.path.join(data_directory, filename)
        print(f"\nProcessing file: {filename}")
        
        # Count total lines (for progress bar)
        with open(filepath, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        
        # Read and insert documents in batches
        with open(filepath, 'r', encoding='utf-8') as f:
            batch = []
            inserted_count = 0
            
            for line in tqdm(f, total=total_lines, desc=f"Inserting {filename}"):
                try:
                    doc = json.loads(line.strip())
                    batch.append(doc)
                    
                    # Insert when batch reaches batch_size
                    if len(batch) >= 100000:
                        result = collection.insert_many(batch)
                        inserted_count += len(result.inserted_ids)
                        batch = []
                except json.JSONDecodeError as e:
                    print(f"\nError decoding JSON in {filename}: {e}")
                    continue
            
            # Insert remaining documents in the batch
            if batch:
                result = collection.insert_many(batch)
                inserted_count += len(result.inserted_ids)
            
            print(f"Successfully inserted {inserted_count} documents from {filename}")
    
    print("\nAll files processed successfully!")
    client.close()

def main():
    # Configuration variables - modify these as needed
    DATA_DIRECTORY = "./data"                   # Directory containing your .jsonl files
    DB_NAME = "wikipedia"                       # MongoDB database name
    COLLECTION_NAME = "pages"                   # MongoDB collection name
 
    # Run the import
    load_jsonl_files_to_mongodb(
        data_directory=DATA_DIRECTORY,
        db=DB_NAME,
        collection=COLLECTION_NAME
    )

if __name__ == "__main__":
    main()
