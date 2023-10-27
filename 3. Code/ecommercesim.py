from faker import Faker
from pymongo import MongoClient
from random import choice
import pandas as pd
import numpy as np
from numpy import random
import datetime

# Initialize Faker
fake = Faker()

# MongoDB configuration
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["ecommerce_db"]

# Function to insert customers into MongoDB
def insert_customers(customers):
    customers_collection = db["customers"]
    customers_collection.insert_many(customers)

num_customers = 2000
customers = []
for _ in range(num_customers):
    customer = {
        "Customer ID": fake.uuid4(),
        "First Name": fake.first_name(),
        "Last Name": fake.last_name(),
        "Email": fake.email(),
        "Phone Number": fake.phone_number(),
        "Registration Date": fake.date_this_decade().strftime("%Y-%m-%d"),
        "Age": fake.random_int(min=18, max=80),  # Example: Random age between 18 and 80
        "Gender": fake.random_element(elements=("Male", "Female", "Other")),  # Example: Random gender
        "Location": {
            "Street": fake.street_address(),
            "City": fake.city(),
            "State": fake.state_abbr(),
            "Postal Code": fake.zipcode(),
            "Country": fake.country()
        },
        "Purchase Frequency": fake.random_int(min=1, max=50),  # Example: Random purchase frequency
        "Average Order Value": round(random.uniform(10, 200), 2),  # Example: Random order value
        "Recency of Purchase": fake.random_int(min=1, max=365),  # Example: Random days since last purchase
        "Average Items per Order": fake.random_int(min=1, max=10),  # Example: Random items per order
        "Loyalty Program Membership": fake.boolean(),  # Example: Random loyalty program membership
        "Online Shopping": fake.boolean(),  # Example: Random online shopping preference
        "Open Rate of Emails": round(random.uniform(0, 1), 2),  # Example: Random email open rate
        "Click-Through Rate": round(random.uniform(0, 1), 2),  # Example: Random click-through rate
        "Total Spending": round(random.uniform(100, 2000), 2),  # Example: Random total spending
        "Repeat Purchases": fake.random_int(min=1, max=10),  # Example: Random repeat purchases
        "Social Media Engagement": round(random.uniform(0, 1), 2),  # Example: Random social media engagement
        "Customer Feedback": round(random.uniform(0, 5), 2)  # Example: Random feedback rating
    }
    customers.append(customer)

# Insert generated customers into MongoDB
insert_customers(customers)

print("Customers data insertion completed.")

# Function to insert products into MongoDB
def insert_products(products):
    products_collection = db["products"]
    products_collection.insert_many(products)
    
num_products = 500
product_ids = [f'P{i+1:05d}' for i in range(num_products)]

# List of product names for each sub-category
product_names = {
    'Mobile Phones': ['iPhone 13', 'Samsung Galaxy S21', 'Google Pixel 6', 'OnePlus 9', 'Xiaomi Mi 11', 'Sony Xperia 1 III'],
    'Laptops': ['Dell XPS 15', 'MacBook Pro', 'HP Spectre x360', 'Lenovo ThinkPad X1', 'Asus ROG Zephyrus', 'Acer Swift 3'],
    'Headphones': ['Sony WH-1000XM4', 'Bose QuietComfort 35', 'AirPods Pro', 'Jabra Elite 85t', 'Sennheiser HD 660S', 'Beats Studio Buds'],
    'Cameras': ['Canon EOS 5D Mark IV', 'Sony Alpha A7 III', 'Nikon Z6 II', 'Fujifilm X-T4', 'Panasonic Lumix GH5', 'Olympus OM-D E-M5 Mark III'],
    'T-Shirts': ['Basic Cotton T-Shirt', 'Graphic Print Tee', 'Striped Polo Shirt', 'V-Neck Solid Color Tee', 'Athletic Performance Shirt'],
    'Jeans': ['Slim Fit Denim Jeans', 'Distressed Skinny Jeans', 'High-Waisted Mom Jeans', 'Bootcut Jeans', 'Straight Leg Jeans'],
    'Dresses': ['Maxi Sundress', 'Wrap Dress', 'Off-Shoulder Midi Dress', 'Fit-and-Flare Party Dress', 'Boho Floral Dress'],
    'Sweaters': ['Crewneck Wool Sweater', 'Turtleneck Cashmere Sweater', 'Cardigan with Buttons', 'Chunky Knit Pullover'],
    'Fiction': ['1984 by George Orwell', 'To Kill a Mockingbird by Harper Lee', 'The Great Gatsby by F. Scott Fitzgerald', 'Pride and Prejudice by Jane Austen'],
    'Non-Fiction': ['Sapiens by Yuval Noah Harari', 'Becoming by Michelle Obama', 'The Power of Habit by Charles Duhigg', 'Thinking, Fast and Slow by Daniel Kahneman'],
    'Mystery': ['The Girl on the Train by Paula Hawkins', 'Gone Girl by Gillian Flynn', 'The Da Vinci Code by Dan Brown', 'Big Little Lies by Liane Moriarty'],
    'Self-Help': ['The Subtle Art of Not Giving a Fuck by Mark Manson', 'You Are a Badass by Jen Sincero', 'Atomic Habits by James Clear', 'Daring Greatly by Brené Brown'],
    'Skincare': ['Cleansing Oil', 'Hydrating Serum', 'Retinol Cream', 'Sheet Masks Set', 'Sunscreen SPF 50+'],
    'Makeup': ['Foundation with SPF', 'Eyeshadow Palette', 'Liquid Lipstick', 'Highlighter Stick', 'Mascara'],
    'Fragrance': ['Eau de Parfum Spray', 'Citrus Cologne', 'Floral Perfume', 'Woody Eau de Toilette'],
    'Haircare': ['Shampoo for Dry Hair', 'Conditioner for Damaged Hair', 'Hair Oil Treatment', 'Hair Mask', 'Heat Protectant Spray'],
    'Furniture': ['Mid-Century Modern Sofa', 'Solid Wood Dining Table', 'Upholstered Accent Chair', 'Platform Bed with Storage'],
    'Kitchenware': ['Stainless Steel Cookware Set', 'Non-Stick Frying Pan', 'Cutlery Knife Block Set', 'Coffee Maker', 'Blender'],
    'Decor': ['Artificial Plants Set', 'Decorative Throw Pillows', 'Wall Clock', 'Ceramic Vases', 'Canvas Wall Art'],
    'Appliances': ['Robot Vacuum Cleaner', 'Air Purifier', 'Smart Thermostat', 'Espresso Machine', 'Food Processor']
}

product_name_weights = {
    'iPhone 13': 0.7,
    'Samsung Galaxy S21': 0.6,
    'Google Pixel 6': 0.5,
    'OnePlus 9': 0.4,
    'Xiaomi Mi 11': 0.3,
    'Sony Xperia 1 III': 0.3,
    'Dell XPS 15': 0.8,
    'MacBook Pro': 0.9,
    'HP Spectre x360': 0.6,
    'Lenovo ThinkPad X1': 0.5,
    'Asus ROG Zephyrus': 0.4,
    'Acer Swift 3': 0.3,
    'Sony WH-1000XM4': 0.8,
    'Bose QuietComfort 35': 0.7,
    'AirPods Pro': 0.5,
    'Jabra Elite 85t': 0.4,
    'Sennheiser HD 660S': 0.4,
    'Beats Studio Buds': 0.3,
    'Canon EOS 5D Mark IV': 0.8,
    'Sony Alpha A7 III': 0.7,
    'Nikon Z6 II': 0.6,
    'Fujifilm X-T4': 0.4,
    'Panasonic Lumix GH5': 0.3,
    'Olympus OM-D E-M5 Mark III': 0.2,
    'Basic Cotton T-Shirt': 0.2,
    'Graphic Print Tee': 0.3,
    'Striped Polo Shirt': 0.4,
    'V-Neck Solid Color Tee': 0.5,
    'Athletic Performance Shirt': 0.5,
    'Slim Fit Denim Jeans': 0.4,
    'Distressed Skinny Jeans': 0.4,
    'High-Waisted Mom Jeans': 0.3,
    'Bootcut Jeans': 0.4,
    'Straight Leg Jeans': 0.3,
    'Maxi Sundress': 0.4,
    'Wrap Dress': 0.5,
    'Off-Shoulder Midi Dress': 0.6,
    'Fit-and-Flare Party Dress': 0.6,
    'Boho Floral Dress': 0.8,
    'Crewneck Wool Sweater': 0.5,
    'Turtleneck Cashmere Sweater': 0.6,
    'Cardigan with Buttons': 0.7,
    'Chunky Knit Pullover': 0.8,
    '1984 by George Orwell': 0.3,
    'To Kill a Mockingbird by Harper Lee': 0.4,
    'The Great Gatsby by F. Scott Fitzgerald': 0.5,
    'Pride and Prejudice by Jane Austen': 0.6,
    'Sapiens by Yuval Noah Harari': 0.6,
    'Becoming by Michelle Obama': 0.7,
    'The Power of Habit by Charles Duhigg': 0.7,
    'Thinking, Fast and Slow by Daniel Kahneman': 0.8,
    'The Girl on the Train by Paula Hawkins': 0.5,
    'Gone Girl by Gillian Flynn': 0.6,
    'The Da Vinci Code by Dan Brown': 0.7,
    'Big Little Lies by Liane Moriarty': 0.8,
    'The Subtle Art of Not Giving a Fuck by Mark Manson': 0.6,
    'You Are a Badass by Jen Sincero': 0.7,
    'Atomic Habits by James Clear': 0.7,
    'Daring Greatly by Brené Brown': 0.8,
    'Cleansing Oil': 0.2,
    'Hydrating Serum': 0.3,
    'Retinol Cream': 0.4,
    'Sheet Masks Set': 0.5,
    'Sunscreen SPF 50+': 0.6,
    'Foundation with SPF': 0.3,
    'Eyeshadow Palette': 0.4,
    'Liquid Lipstick': 0.5,
    'Highlighter Stick': 0.6,
    'Mascara': 0.7,
    'Eau de Parfum Spray': 0.4,
    'Citrus Cologne': 0.5,
    'Floral Perfume': 0.6,
    'Woody Eau de Toilette': 0.7,
    'Shampoo for Dry Hair': 0.3,
    'Conditioner for Damaged Hair': 0.4,
    'Hair Oil Treatment': 0.5,
    'Hair Mask': 0.6,
    'Heat Protectant Spray': 0.7,
    'Mid-Century Modern Sofa': 0.5,
    'Solid Wood Dining Table': 0.6,
    'Upholstered Accent Chair': 0.7,
    'Platform Bed with Storage': 0.8,
    'Stainless Steel Cookware Set': 0.5,
    'Non-Stick Frying Pan': 0.6,
    'Cutlery Knife Block Set': 0.7,
    'Coffee Maker': 0.8,
    'Blender': 0.7,
    'Artificial Plants Set': 0.3,
    'Decorative Throw Pillows': 0.4,
    'Wall Clock': 0.5,
    'Ceramic Vases': 0.6,
    'Canvas Wall Art': 0.7,
    'Robot Vacuum Cleaner': 0.6,
    'Air Purifier': 0.7,
    'Smart Thermostat': 0.8,
    'Espresso Machine': 0.7,
    'Food Processor': 0.8
}

product_categories = {
    'Electronics': ['Mobile Phones', 'Laptops', 'Headphones', 'Cameras'],
    'Clothing': ['T-Shirts', 'Jeans', 'Dresses', 'Sweaters'],
    'Books': ['Fiction', 'Non-Fiction', 'Mystery', 'Self-Help'],
    'Beauty': ['Skincare', 'Makeup', 'Fragrance', 'Haircare'],
    'Home': ['Furniture', 'Kitchenware', 'Decor', 'Appliances']
}

product_categories.items()
product_ids = [f'P{i+1:05d}' for i in range(num_products)]
def generate_product_data(num_products):
    categories = list(product_categories.keys())
    sub_categories = {category: sub_cats for category, sub_cats in product_categories.items()}

    product_data = []
    product_id_counter = 1

    for i in range(num_products):
        category = np.random.choice(categories)
        sub_category = np.random.choice(sub_categories[category])
        product_names_sub = product_names[sub_category]
        product_name_weights_sub = [product_name_weights[name] for name in product_names_sub]

        
        # Normalize the weights to ensure they sum up to 1
        weight_sum = sum(product_name_weights_sub)
        normalized_weights = [weight / weight_sum for weight in product_name_weights_sub]
       
        product_name = np.random.choice(product_names_sub, p=normalized_weights)
        unit_price = np.random.uniform(10, 500)  # Adjust the range as needed
       
        
        
        # Calculate a random selling price based on unit price
        profit_margin = np.random.uniform(1.2, 1.5)
        selling_price = round(unit_price * profit_margin, 2)

        product_data.append({
            'Product_ID': f'P{product_id_counter:04d}',
            'Product_Name': product_name,
            'Category': category,
            'Sub_Category': sub_category,
            'Unit_Price': unit_price,
            'Selling_Price': selling_price
        })

        product_id_counter += 1

    return product_data

product = generate_product_data(num_products)
    
insert_products(product)




# Function to insert orders into MongoDB
def insert_orders(orders):
    orders_collection = db["orders"]
    orders_collection.insert_many(orders)

# Generate synthetic data for Orders

def fake_order_date_last_3_years():
    current_year = datetime.datetime.now().year
    random_year = random.randint(current_year - 3, current_year - 1)
    order_date = fake.date_time_between_dates(
        datetime_start=datetime.datetime(random_year, 1, 1),
        datetime_end=datetime.datetime(random_year, 12, 31)
    )
    return order_date

num_orders = 100
orders = []
customer_ids = [customer["Customer ID"] for customer in customers]  # Use customers generated earlier
for i in range(num_orders):
    order = {
        "Order ID": fake.uuid4(),
        "Customer ID": choice(customer_ids),
        "Order Date": fake_order_date_last_3_years(),
        "Total Amount": round(fake.random.uniform(50, 1000), 2),
        "Payment Method": fake.random_element(elements=["Credit Card", "PayPal", "COD"]),
        "Shipping Address": {
            "Street": fake.street_address(),
            "City": fake.city(),
            "State": fake.state_abbr(),
            "Postal Code": fake.zipcode(),
            "Country": fake.country()
        },
        "Order Status": fake.random_element(elements=["Pending", "Shipped", "Delivered"])
    }
    orders.append(order)

# Insert generated orders into MongoDB
insert_orders(orders)

print("Orders data insertion completed.")


# Function to insert order items into MongoDB
def insert_order_items(order_items):
    order_items_collection = db["order_items"]
    order_items_collection.insert_many(order_items)

# Generate synthetic data for Order Items
order_items = []
for order in orders:  # Use orders generated earlier
    num_items = fake.random_int(min=1, max=5)
    for _ in range(num_items):
        order_item = {
            "Order Item ID": fake.uuid4(),
            "Order ID": order["Order ID"],
            "Product ID": choice([p["Product_ID"] for p in product]),
            "Quantity": fake.random_int(min=1, max=10),
            "Subtotal": round(fake.random.uniform(10, 500), 2)
        }
        order_items.append(order_item)

# Insert generated order items into MongoDB
insert_order_items(order_items)

print("Order Items data insertion completed.")

# Function to insert reviews into MongoDB
def insert_reviews(reviews):
    reviews_collection = db["reviews"]
    reviews_collection.insert_many(reviews)

# Generate synthetic data for Reviews
reviews = []
for order_item in order_items:  # Use order items generated earlier
    review = {
        "Review ID": fake.uuid4(),
        "Customer ID": choice(customer_ids),
        "Product ID": order_item["Product ID"],
        "Rating": fake.random_int(min=1, max=5),
        "Text": fake.paragraph(),
        "Timestamp": fake.date_time_between(start_date="-1y", end_date="now")
    }
    reviews.append(review)

# Insert generated reviews into MongoDB
insert_reviews(reviews)

print("Reviews data insertion completed.")

# Function to insert refunds into MongoDB
def insert_refunds(refunds):
    refunds_collection = db["refunds"]
    refunds_collection.insert_many(refunds)

# Generate synthetic data for Refunds
refunds = []
for order in orders:  # Use orders generated earlier
    if fake.boolean(chance_of_getting_true=20):
        refund = {
            "Refund ID": fake.uuid4(),
            "Order ID": order["Order ID"],
            "Refund Date": fake.date_time_between_dates(order["Order Date"], "now"),
            "Refunded Amount": round(fake.random.uniform(10, order["Total Amount"]), 2),
            "Reason": fake.sentence()
        }
        refunds.append(refund)

# Insert generated refunds into MongoDB
insert_refunds(refunds)

print("Refunds data insertion completed.")

# Function to insert web traffic data into MongoDB
def insert_web_traffic(web_traffic_data):
    web_traffic_collection = db["web_traffic"]
    web_traffic_collection.insert_many(web_traffic_data)

# Generate synthetic data for Web Traffic
num_web_traffic_entries = 500
web_traffic_data = []
for _ in range(num_web_traffic_entries):
    entry = {
        "Timestamp": fake.date_time_between(start_date="-1d", end_date="now"),
        "Page Visited": fake.url(),
        "Referral Source": fake.domain_name()
    }
    web_traffic_data.append(entry)

# Insert generated web traffic data into MongoDB
insert_web_traffic(web_traffic_data)

print("Web Traffic data insertion completed.")

# Function to insert marketing campaigns into MongoDB
def insert_marketing_campaigns(marketing_campaigns):
    marketing_campaigns_collection = db["marketing_campaigns"]
    marketing_campaigns_collection.insert_many(marketing_campaigns)

# Generate synthetic data for Marketing Campaigns
marketing_campaigns = []
for i in range(1, 4):  # Generate 3 campaigns
    campaign = {
        "Campaign ID": f"CM{i}",
        "Campaign Name": f"Campaign {i}",
        "Start Date": fake.date_time_this_year(),
        "End Date": fake.date_time_this_year()
    }
    marketing_campaigns.append(campaign)

# Insert generated marketing campaigns into MongoDB
insert_marketing_campaigns(marketing_campaigns)

print("Marketing Campaigns data insertion completed.")

# Function to insert campaign metrics into MongoDB
def insert_campaign_metrics(campaign_metrics):
    campaign_metrics_collection = db["campaign_metrics"]
    campaign_metrics_collection.insert_many(campaign_metrics)

# Generate synthetic data for Campaign Metrics
campaign_metrics = []
for campaign in marketing_campaigns:  # Use marketing campaigns generated earlier
    metric = {
        "Metric ID": fake.uuid4(),
        "Campaign ID": campaign["Campaign ID"],
        "Clicks": fake.random_int(min=10, max=1000),
        "Impressions": fake.random_int(min=100, max=10000),
        "Conversions": fake.random_int(min=1, max=50)
    }
    campaign_metrics.append(metric)

# Insert generated campaign metrics into MongoDB
insert_campaign_metrics(campaign_metrics)

print("Campaign Metrics data insertion completed.")


# Function to insert locations into MongoDB
def insert_locations(locations):
    locations_collection = db["locations"]
    locations_collection.insert_many(locations)

# Generate synthetic data for Locations
num_locations = 10
locations = []
for i in range(num_locations):
    location = {
        "Location ID": f"L{i+1}",
        "Country": fake.country(),
        "State": fake.state(),
        "City": fake.city(),
        "Address": fake.street_address(),
        "Street": fake.street_name(),
        "Postal Code": fake.zipcode(),
        "Coordinates": {
            "Latitude": float(fake.latitude()),
            "Longitude": float(fake.longitude())
        }
    }
    locations.append(location)

# Insert generated locations into MongoDB
insert_locations(locations)

print("Locations data insertion completed.")


# Function to insert inventory data into MongoDB
def insert_inventory(inventory):
    inventory_collection = db["inventory"]
    inventory_collection.insert_many(inventory)

# Generate synthetic data for Inventory
inventory = []
for p in product:  # Use products generated earlier
    for location in locations:  # Use locations generated earlier
        item = {
            "Product ID": p["Product_ID"],
            "Location ID": location["Location ID"],
            "Quantity Available": fake.random_int(min=0, max=100)
        }
        inventory.append(item)

# Insert generated inventory data into MongoDB
insert_inventory(inventory)

print("Inventory data insertion completed.")

# Close MongoDB connection
mongo_client.close()

