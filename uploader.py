
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
PRICER_APP=True
last_processed_date = None
buckets = os.getenv("BUCKET_NAME", BUCKER)
exchange = os.getenv("EXCHANGE", EXCHANGE)
TRADING_PAIRS = os.getenv("TRADING_PAIRS", TRADING_PAIRS)
BACKWARD_TIME = os.getenv("BACKWARD_TIME", None)
PRICER_APP_TMP = os.getenv("PRICER_APP", None)
if PRICER_APP_TMP and PRICER_APP_TMP.lower() == "true":
    PRICER_APP = True
BASE_PATH = os.getenv("BASE_PATH", os.path.dirname(os.path.abspath(__file__)))

base_path = BASE_PATH

trading_pairs = TRADING_PAIRS.split(",") if TRADING_PAIRS else []

PLUTUS_BUCKET = os.getenv("PLUTUS_BUCKET", "cypher-market-data-plutus-daily")
INSRUCMENT_TYPE = os.getenv("INSTRUMENT_TYPE", "spot")

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
async def upload_file_to_s3(file_path, bucket=None, key=None):
    try:
        # Upload the file to S3
        if key is None:
            key = os.path.basename(file_path)
        if bucket is None:
            bucket = BUCKER
        s3.upload_file(file_path, bucket, key)
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

def cache_last_processed_date(date):
    global last_processed_date
    last_processed_date = date
    logging.info(f"Last processed date cached: {last_processed_date}")
    # You can also save this to a file or database if needed
    with open(BASE_PATH + "/" + "last_processed_date.txt", "w") as f:
        f.write(date.strftime("%Y/%m/%d/%H"))
def get_last_processed_date():
    global last_processed_date
    if last_processed_date is not None:
        return last_processed_date
    try:
        with open(BASE_PATH + "/" + "last_processed_date.txt", "r") as f:
            date_str = f.read().strip()
            last_processed_date = datetime.strptime(date_str, "%Y/%m/%d/%H")
            logging.info(f"Last processed date loaded: {last_processed_date}")
            return last_processed_date
    except FileNotFoundError:
        logging.warning("Last processed date file not found. Returning None.")
        return None
def compress(file, directory=None, new_name=None):
    import gzip
    import shutil
    output = directory + "/"+ new_name + ".gz"
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


def get_file_path():
    restuls = []
    for root, dirs, files in os.walk(base_path):
       print(f"Directory: {root}")
       for file in files:
           if file == "last_processed_date.txt":
               continue
           print(f"  File: {root}/{file}")
           time = f"{os.path.relpath(root, base_path)}:00:00"
           relative_path = os.path.relpath(root, base_path)
           print(f" reletive path: {os.path.relpath(root, base_path)}")
           timestamp = datetime.strptime(time, "%Y/%m/%d/%H:%M:%S")
           print(f"  Timestamp: {timestamp}")
           restuls.append({
               "directory": root,
               "file": file,
               "file_path": os.path.join(root, file),
               "timestamp": timestamp,
               "relative_path": relative_path
           })
            # You can implement directory processing logic here if needed
    return restuls



async def pricer_main():
    last_processed_date = get_last_processed_date()
    bucket = f"s3://{PLUTUS_BUCKET}/{exchange}/{INSRUCMENT_TYPE}"
    logging.info(f"Starting pricer_main with bucket: {bucket} and last processed date: {last_processed_date}")
    try:
        while True:
            yesterday= datetime.now() - timedelta(days=1)
            files = get_file_path()
            files = [f for f in files if f["timestamp"].date() <= yesterday.date() and f["file"] != "last_processed_date.txt" and f["file"].endswith(".bin")]
            if last_processed_date:
                files = [f for f in files if f["timestamp"] > last_processed_date]
            files = sorted(files, key=lambda x: x["timestamp"])
            print(f"Files to process: {files}")
            logging.info(f"Files to process: {files}")
            for file_info in files:
                file_path = file_info["file_path"]
                directory = file_info["directory"]
                relative_path = file_info["relative_path"]
                timestamp = file_info["timestamp"]
                s3_file_key = f"{exchange}_{INSRUCMENT_TYPE}_{trading_pairs[0]}_{datetime.strftime(timestamp, '%Y_%m_%d_%H')}"
                s3_file_path = compress(file_path, directory=directory, new_name=s3_file_key)
                logging.info(f"Processing file: {file_path} with key: {s3_file_key} path: {s3_file_path}")
                object_name = f"{exchange}/{INSRUCMENT_TYPE}/{s3_file_key}.gz"
                await upload_file_to_s3(s3_file_path, PLUTUS_BUCKET, key=object_name)
                last_processed_date = timestamp
                cache_last_processed_date(last_processed_date)
                await asyncio.sleep(10)  # Simulate the upload time
            await asyncio.sleep(86400)  # Simulate the upload time
    except Exception as e:
        logging.error(f"An error occurred during file processing: {e}")
        return
async def hummingbot_main():
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
        await asyncio.sleep(86400)

async def main():
    if PRICER_APP:
        await pricer_main()
    else:
        await hummingbot_main()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Process interrupted by user. Exiting...")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        cache_last_processed_date(last_processed_date)
        sys.exit(1)
    finally:
        cache_last_processed_date(last_processed_date)
        logging.info("Uploader script has finished execution.")