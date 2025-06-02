#!/bin/bash
sleep 10

echo "Waiting for MongoDB instances to start..."
sleep 30

echo "Initiating replica set..."
mongosh --host mongo:27017 -u root -p rootpassword --authenticationDatabase admin --eval "
try {
  rs.status();
  print('Replica set already initialized');
} catch (err) {
  print('Initializing replica set...');
  rs.initiate({
    _id: 'rs0',
    members: [
      {_id: 0, host: 'mongo:27017', priority: 1},
      {_id: 1, host: 'second:27017', priority: 0.5},
      {_id: 2, host: 'third:27017', priority: 0.5}
    ]
  });
}
"
