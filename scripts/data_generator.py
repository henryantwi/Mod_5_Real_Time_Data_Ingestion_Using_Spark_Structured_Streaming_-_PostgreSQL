"""
E-Commerce Event Data Generator
================================
This script generates fake e-commerce user events and saves them as CSV files.
The CSV files are used by Spark Structured Streaming for real-time processing.

Usage:
    python data_generator.py [--events N] [--interval SECONDS] [--continuous]
"""

import os
from typing import Any
import csv
import uuid
import random
import argparse
from datetime import datetime
from time import sleep
from faker import Faker

# Initialize Faker
fake = Faker()

# Configuration - OUTPUT_DIR can be overridden via environment variable
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/opt/spark/work-dir/data")
EVENT_TYPES = ["view", "add_to_cart", "remove_from_cart", "purchase", "wishlist"]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports", 
    "Books", "Toys", "Beauty", "Food & Beverages"
]

# Sample products for realistic data
PRODUCTS = [
    {"id": "PROD001", "name": "Wireless Bluetooth Headphones", "category": "Electronics", "price": 79.99},
    {"id": "PROD002", "name": "Cotton T-Shirt", "category": "Clothing", "price": 24.99},
    {"id": "PROD003", "name": "Smart LED Light Bulb", "category": "Home & Garden", "price": 15.99},
    {"id": "PROD004", "name": "Yoga Mat Pro", "category": "Sports", "price": 45.00},
    {"id": "PROD005", "name": "Python Programming Book", "category": "Books", "price": 39.99},
    {"id": "PROD006", "name": "Building Blocks Set", "category": "Toys", "price": 29.99},
    {"id": "PROD007", "name": "Organic Face Cream", "category": "Beauty", "price": 34.99},
    {"id": "PROD008", "name": "Gourmet Coffee Beans", "category": "Food & Beverages", "price": 18.99},
    {"id": "PROD009", "name": "Gaming Mouse", "category": "Electronics", "price": 59.99},
    {"id": "PROD010", "name": "Running Shoes", "category": "Sports", "price": 89.99},
    {"id": "PROD011", "name": "Smartphone Case", "category": "Electronics", "price": 19.99},
    {"id": "PROD012", "name": "Winter Jacket", "category": "Clothing", "price": 129.99},
    {"id": "PROD013", "name": "Garden Tool Set", "category": "Home & Garden", "price": 49.99},
    {"id": "PROD014", "name": "Data Science Handbook", "category": "Books", "price": 54.99},
    {"id": "PROD015", "name": "Wireless Charger", "category": "Electronics", "price": 29.99},
]

# Simulated users (cached for session consistency)
USER_SESSIONS = {}


def generate_user_id():
    """Generate or return an existing user ID for session consistency."""
    if random.random() < 0.7 and USER_SESSIONS:  # 70% chance to reuse existing user
        return random.choice(list(USER_SESSIONS.keys()))
    
    # Generate standard UUID format for PostgreSQL UUID column
    user_id = str(uuid.uuid4())
    USER_SESSIONS[user_id] = {
        "session_id": str(uuid.uuid4()),  # Standard UUID format
        "device": random.choice(DEVICE_TYPES)
    }
    return user_id


def generate_event():
    """Generate a single e-commerce event."""
    user_id = generate_user_id()
    user_session = USER_SESSIONS[user_id]
    product = random.choice(PRODUCTS)
    
    # Weight event types (views are most common)
    event_weights = [0.5, 0.2, 0.05, 0.15, 0.1]  # view, add_to_cart, remove, purchase, wishlist
    event_type = random.choices(EVENT_TYPES, weights=event_weights)[0]
    
    # Quantity is only relevant for purchases and add_to_cart
    quantity = random.randint(1, 3) if event_type in ["purchase", "add_to_cart"] else 1
    
    return {
        "event_id": str(uuid.uuid4()),  # Standard UUID format for PostgreSQL
        "user_id": user_id,
        "event_type": event_type,
        "product_id": product["id"],
        "product_name": product["name"],
        "product_category": product["category"],
        "product_price": product["price"],
        "quantity": quantity,
        "event_timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "session_id": user_session["session_id"],
        "device_type": user_session["device"]
    }


def generate_batch(num_events: int) -> list[dict[str, str | int | float | datetime]]:
    """Generate a batch of events."""
    return [generate_event() for _ in range(num_events)]


def save_to_csv(events: list[dict[str, str | int | float | datetime]], batch_number: int) -> str:
    """Save events to a CSV file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"events_batch_{batch_number:04d}_{timestamp}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    fieldnames = [
        "event_id", "user_id", "event_type", "product_id", "product_name",
        "product_category", "product_price", "quantity", "event_timestamp",
        "session_id", "device_type"
    ]
    
    with open(filepath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)
    
    return filepath


def main():
    parser = argparse.ArgumentParser(description="Generate e-commerce event data")
    parser.add_argument("--events", type=int, default=10, 
                        help="Number of events per batch (default: 10)")
    parser.add_argument("--interval", type=float, default=5.0,
                        help="Seconds between batches (default: 5.0)")
    parser.add_argument("--batches", type=int, default=1,
                        help="Number of batches to generate (default: 1)")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously until interrupted")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("E-Commerce Event Data Generator")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Events per batch: {args.events}")
    print(f"Interval between batches: {args.interval} seconds")
    print(f"Mode: {'Continuous' if args.continuous else f'{args.batches} batch(es)'}")
    print("=" * 60)
    
    batch_number = 0
    total_events = 0
    
    try:
        while True:
            batch_number += 1
            events = generate_batch(args.events)
            filepath = save_to_csv(events, batch_number)
            total_events += len(events)
            
            print(f"[Batch {batch_number:04d}] Generated {len(events)} events -> {os.path.basename(filepath)}")
            print(f"  Total events generated: {total_events}")
            
            if not args.continuous and batch_number >= args.batches:
                break
            
            sleep(args.interval)
            
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("Generator stopped by user")
        print(f"Total batches: {batch_number}")
        print(f"Total events: {total_events}")
        print("=" * 60)


if __name__ == "__main__":
    main()
