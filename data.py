import os
from dotenv import load_dotenv
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

def main():
    
    setup_kaggle()  # Download dataset from Kaggle
    # Step 2: Process and insert into MongoDB
    

if __name__ == '__main__':
    main()
