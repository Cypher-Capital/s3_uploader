
import asyncio
import logging
import os
import sys
import boto3
import json
from datetime import datetime, timedelta

BUCKER = "cypher-trades-ob-daily-data"
TRADING_PAIRS= "POL-USDT,ETH-USDT"
EXCHANGE="binance"

buckets = os.getenv("BUCKET_NAME", BUCKER)
exchange = os.getenv("EXCHANGE", EXCHANGE)
TRADING_PAIRS = os.getenv("TRADING_PAIRS", TRADING_PAIRS)
BACKWARD_TIME = os.getenv("BACKWARD_TIME", None)

BASE_PATH = os.getenv("BASE_PATH", os.path.dirname(os.path.abspath(__file__)))

base_path = BASE_PATH

trading_pairs = TRADING_PAIRS.split(",") if TRADING_PAIRS else []

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("uploader.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
# Initialize the S3 client
s3 = boto3.client("s3")
# Function to upload a file to S3
async def upload_file_to_s3(file_path):
    try:
        # Upload the file to S3
        s3.upload_file(file_path, BUCKER, os.path.basename(file_path))
        logging.info(f"File {file_path} uploaded to S3 bucket {BUCKER}.")
        return True
    except Exception as e:
        logging.error(f"Failed to upload file {file_path} to S3: {e}")
        return False



def compressfiles(file):
    import gzip
    import shutil
    output = file + ".gz"
    with open(file, 'rb') as f_in:
        with gzip.open(output, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return output


async def process_pair(trading_pair, exchange, base_path, date_time):
    products = ("order_book_snapshots", "trades")
    for product in products:
        file_path = base_path + f"/{exchange}_{trading_pair}_{product}_{date_time}.txt"
        if not os.path.exists(file_path):
            logging.error(f"File {file_path} does not exist.")
            continue
        zipped_file = compressfiles(file_path)
        result = await upload_file_to_s3(zipped_file)
        if result:
            logging.info(f"File {file_path} uploaded successfully.")
            os.remove(file_path)
            os.remove(zipped_file)
        else:
            logging.error(f"Failed to upload file {file_path}.")


async def main():
    # This is the main function that will be called when the script is run
    # It will handle the file upload process
    backward_time = BACKWARD_TIME
    yesterday= datetime.now() - timedelta(days=1)
    while True:
        await asyncio.sleep(60)  # Simulate some async work
        logging.info("Starting the file upload process...")

        today = datetime.now()
        logging.info(f"Current date: {today.date()}")
        logging.info(f"Yesterday's date: {yesterday.date()}")
        if today.date() == yesterday.date():
            logging.info("still in today to process.")
            continue
        processed_date = today - timedelta(days=1) if backward_time is None else datetime.strptime(backward_time, "%Y-%m-%d")
        logging.info(f"Processed date: {processed_date}")
        current_date = today - timedelta(days=1)
        logging.info(f"Current date: {current_date}")
        while current_date >= processed_date:
            current_date_str = datetime.strftime(current_date, "%Y-%m-%d")
            logging.info(f"Processing date: {current_date_str}")
            for trading_pair in trading_pairs:
                logging.info(f"All files have been processing. {trading_pair} {exchange} {base_path} {current_date_str}")
                await process_pair(trading_pair, exchange, base_path, current_date_str)
                
                # Here you would implement the actual upload logic
                # For example, you might want to read a file and upload it to a server
                # For now, we'll just simulate a successful upload
                await asyncio.sleep(1)  # Simulate the upload time
                logging.info(f"File upload completed successfully {trading_pair}.")
            current_date -= timedelta(days=1)
        backward_time = None
        yesterday = today
        logging.info("waiting for 1 days")
        await asyncio.sleep(60)
        # Sleep for a day before checking again
        # You can adjust the sleep time as needed
        # For example, you might want to check for new files every hour
        # or every minute instead of every day
        # This is just a placeholder for the actual file upload logic
        # You can replace this with your own implementation
        # For example, you might want to read a file and upload it to a server
        # For now, we'll just simulate a successful upload
# Set up logging

if __name__ == "__main__":
    asyncio.run(main())