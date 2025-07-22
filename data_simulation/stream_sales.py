import random
import json
import boto3
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
import uuid
import time
s3=boto3.client('s3')
BUCKET_NAME=os.getenv("S3_BUCKET_NAME")
STREAM_PREFIX='sales_stream/'
categories=['Electronics','Clothing','Books','Home',"Toys"]
products={
    'Electronics': ['Laptop', 'Smartphone', 'Headphones'],
    'Clothing': ['T-Shirt', 'Jeans', 'Jacket'],
    'Books': ['Fiction', 'Biography', 'Science'],
    'Home': ['Mixer', 'Toaster', 'Vacuum Cleaner'],
    'Toys': ['Action Figure', 'Board Game', 'Puzzle']
}

def generate_sale_record():
    category=random.choice(categories)
    product=random.choice(products[category])
    order_id=str(uuid.uuid4())
    product_id=f"{category[:3].upper()}-{random.randint(1000,9999)}"
    price=round(random.uniform(10.0,50.0),2)
    timestamp=datetime.utcnow() - timedelta(minutes=random.randint(0, 60))
    return {
        'order_id': order_id,
        'product_id': product_id,
        'product_name': product,
        'category': category,
        'price': price,
        'timestamp': timestamp.isoformat()
    }
def stream_sales():
    while True:
        records = [generate_sale_record() for _ in range(5)]  # small batch of 5 records
        now = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        file_name = f"sales_{now}.json"
        

        output_dir = "tmp"
        os.makedirs(output_dir, exist_ok=True)  # create tmp folder if not exists

        local_path = os.path.join(output_dir, f"sales_{now}.json")


        # Saving locally first
        with open(local_path, 'w') as f:
            for record in records:
                json.dump(record, f)
                f.write('\n')

        s3.upload_file(local_path, BUCKET_NAME, STREAM_PREFIX + file_name)
        print(f"[{now}] Uploaded {file_name} to S3 -> {STREAM_PREFIX + file_name}")

        time.sleep(5)  # wait 5 seconds before next batch

if __name__ == "__main__":
    stream_sales()