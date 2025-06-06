from pymongo import MongoClient

if __name__ == '__main__':

    # Connection string format
    uri = "mongodb://root:rootpassword@localhost:27017/?replicaSet=rs0&authSource=admin&directConnection=true"
    print(uri)

    # Connect to the replica set
    client = MongoClient(uri)

    # Access a database
    db = client['wikipedia']  # Use or create a database named 'wikipedia'

    # Example: List collections
    print(db.list_collection_names())
