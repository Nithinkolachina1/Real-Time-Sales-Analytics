import boto3
import json
import random
from datetime import datetime,timedelta
import os
from dotenv import load_dotenv
import uuid
import time
load_dotenv()

#AWS S3 setup
s3=boto3.client('s3')
BUCKET_NAME=os.getenv("S3_BUCKET_NAME")

#Example Products and Categories
CATEGORIES = ['Electronics', 'Clothing', 'Books', 'Home', 'Toys']
PRODUCTS = {
    'Electronics': ['Laptop', 'Smartphone', 'Headphones'],
    'Clothing': ['T-Shirt', 'Jeans', 'Jacket'],
    'Books': ['Fiction', 'Biography', 'Science'],
    'Home': ['Mixer', 'Toaster', 'Vacuum Cleaner'],
    'Toys': ['Action Figure', 'Board Game', 'Puzzle']
}

def generate_sale_record():
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    order_id = str(uuid.uuid4())
    product_id = f"{category[:3].upper()}-{random.randint(1000, 9999)}"
    price = round(random.uniform(10.0, 500.0), 2)
    timestamp = datetime.utcnow() - timedelta(minutes=random.randint(0, 60))

    return {
        'order_id': order_id,
        'product_id': product_id,
        'product_name': product,
        'category': category,
        'price': price,
        'timestamp': timestamp.isoformat()
    }

def main():
    records = [generate_sale_record() for _ in range(100)]  # Generate 100 sales

    file_name = 'sales_data.json'
    # Write JSON lines (one JSON object per line)
    
    with open(file_name, 'w') as f:
        for record in records:
            json.dump(record, f)
            f.write('\n')

    # Upload to S3
    s3.upload_file(file_name, BUCKET_NAME, file_name)
    print(f"Uploaded {file_name} to S3 bucket {BUCKET_NAME}")

if __name__ == "__main__":
    main()
