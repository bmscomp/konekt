// In MongoDB shell:
rs.initiate({
  _id: "rs0",
  members: [{ _id: 0, host: "mongodb:27017" }]
})

db.createRole({
  role: "changeStreamRole",
  privileges: [
    { resource: { db: "", collection: "" }, actions: ["find", "changeStream"] }
  ],
  roles: []
})

db.createUser({
  user: "kafka",
  pwd: "kafkapassword",
  roles: ["changeStreamRole", "readWrite@wikipedia"]
})


db = db.getSiblingDB('store');

// Create application user with readWrite access to store database
db.createUser({
  user: 'storeuser',
  pwd: 'secret',
  roles: [
    { role: 'readWrite', db: 'store' }
  ]
});

// Create collections and insert initial data
db.createCollection('products');
db.createCollection('customers');
db.createCollection('orders');

// Insert sample products
db.products.insertMany([
  { 
    name: 'Laptop', 
    price: 999.99, 
    category: 'Electronics',
    stock: 50,
    createdAt: new Date()
  },
  { 
    name: 'Smartphone', 
    price: 699.99, 
    category: 'Electronics',
    stock: 100,
    createdAt: new Date()
  },
  { 
    name: 'Headphones', 
    price: 149.99, 
    category: 'Accessories',
    stock: 200,
    createdAt: new Date()
  }
]);

// Insert sample customers
db.customers.insertMany([
  {
    name: 'John Doe',
    email: 'john@example.com',
    address: {
      street: '123 Main St',
      city: 'New York',
      zip: '10001'
    },
    createdAt: new Date()
  },
  {
    name: 'Jane Smith',
    email: 'jane@example.com',
    address: {
      street: '456 Oak Ave',
      city: 'Los Angeles',
      zip: '90001'
    },
    createdAt: new Date()
  }
]);
