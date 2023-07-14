import datetime
import logging 
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os, uuid
import requests

import azure.functions as func

url = 'https://www.wroclaw.pl/open-data/f91dd592-95fe-416f-a43e-97838fbb0147/Deszczomierze.csv'
name = "rains" 

def main(mytimer: func.TimerRequest, outqueue: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    logging.info("Start__trigger")

    res = requests.get(url)
    data =  res.text

    outqueue.set(data)
    logging.info(f"Queue has been set")

    conn_str = os.environ.get("jedrzejsmokstorageaccoun_STORAGE")
    
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=name, blob=name + utc_timestamp)
    blob_client.upload_blob(data)
    logging.info(f"Blob {name} has been uploaded")

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
