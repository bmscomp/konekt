# Konekt make you data synched with Kafka and Kafka connect

This project demonstrates a scalable data processing pipeline using Kafka, MongoDB, and Python. It includes several Kafka consumer implementations and a data seeder for MongoDB.

## Prerequisites

- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/install/)
- [Python](https://www.python.org/downloads/) 3.8 or higher
- [Git](https://git-scm.com/downloads) (optional)

## Project Structure

```
.
├── docker-compose.yml          # Docker services configuration
├── requirements.txt            # Python dependencies
├── .gitignore                  # Git ignore file
├── Seeder.py                   # Data seeder for MongoDB
├── ScalableKafkaConsumer.py    # Base Kafka consumer class
├── DynamicScalableKafkaConsumer.py  # Consumer with dynamic scaling
├── PartitionAwareConsumer.py   # Consumer with partition awareness
└── mongo-init/                 # MongoDB initialization scripts
    └── 01-init.js              # Initial data and user setup
```

## Setting Up Docker Environment

### 1. Start Docker Services

The project uses Docker Compose to set up the following services:
- SQL Server
- MongoDB
- Kafka (broker)
- Schema Registry
- Kafka Connect
- Control Center

To start all services:

```bash
docker-compose up -d
```

This command starts all services in detached mode. The `-d` flag runs containers in the background.

### 2. Check Service Status

Verify that all services are running:

```bash
docker-compose ps
```

### 3. Access Service UIs

- **Kafka Control Center**: http://localhost:9021
- **Schema Registry**: http://localhost:8081
- **Kafka Connect**: http://localhost:8083

### 4. Stop Docker Services

When you're done working with the project:

```bash
docker-compose down
```

To remove volumes as well (this will delete all data):

```bash
docker-compose down -v
```

## Setting Up Python Virtual Environment

### 1. Create a Virtual Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Kaggle API (for Seeder.py)

To use the Seeder.py script, you need to set up Kaggle API credentials:

1. Create a Kaggle account if you don't have one: https://www.kaggle.com/
2. Go to your Kaggle account settings and create an API token
3. Download the `kaggle.json` file and place it in `~/.kaggle/` directory:

```bash
# Create the directory if it doesn't exist
mkdir -p ~/.kaggle

# Move the downloaded kaggle.json file
mv path/to/downloaded/kaggle.json ~/.kaggle/

# Set proper permissions
chmod 600 ~/.kaggle/kaggle.json
```

## Running Python Scripts

Ensure your virtual environment is activated before running any scripts.

### 1. Seed Data to MongoDB

```bash
python Seeder.py
```

This script:
- Authenticates with the Kaggle API
- Downloads a dataset from Kaggle
- Processes JSON files (supports both JSON arrays and JSON Lines format)
- Inserts the data into MongoDB in batches

### 2. Run Kafka Consumers

#### Basic Scalable Consumer

```bash
python ScalableKafkaConsumer.py
```

#### Dynamic Scalable Consumer

```bash
python DynamicScalableKafkaConsumer.py
```

#### Partition-Aware Consumer

```bash
python PartitionAwareConsumer.py
```

## Customizing Consumers

You can customize the consumer behavior by modifying the following parameters in the scripts:

- `TOPIC`: The Kafka topic to consume from
- `BOOTSTRAP_SERVERS`: Kafka broker addresses
- `GROUP_ID`: Consumer group ID
- `NUM_WORKERS`: Number of worker threads (for ScalableKafkaConsumer)
- `min_workers` and `max_workers`: Worker thread limits (for DynamicScalableKafkaConsumer)
- `num_workers_per_partition`: Workers per partition (for PartitionAwareConsumer)

## MongoDB Connection Details

- **Host**: localhost
- **Port**: 27017
- **Username**: storeuser
- **Password**: secret
- **Database**: store

## Troubleshooting

### Docker Issues

1. **Services not starting**: Check Docker logs with `docker-compose logs [service_name]`
2. **Port conflicts**: Ensure the required ports (1433, 27017, 9092, etc.) are not in use

### Python Issues

1. **Module not found errors**: Ensure all dependencies are installed with `pip install -r requirements.txt`
2. **Kafka connection errors**: Verify Kafka broker is running with `docker-compose ps broker`
3. **MongoDB connection errors**: Check MongoDB status with `docker-compose ps mongo`

## Extending the Project

- Add new consumer implementations by extending the base `ScalableKafkaConsumer` class
- Modify the MongoDB initialization script to create additional collections
- Add new data sources to the Seeder.py script
