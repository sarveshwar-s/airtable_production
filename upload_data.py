
import boto3
import uuid

def create_bucket_name(bucket_name: str):
    return ''.join([bucket_name, uuid.uuid4()])

def upload_files():

    s3_resource = boto3.client('s3')

    bucket_name = "luko-data-eng-exercice"


    web_event_file = "data/web_event.csv"
    app_event_file = "data/app_event.csv"

    # Upload file to bucket
    s3_resource.upload_file(web_event_file, bucket_name, "chohan/web_event.csv")
    print("uploaded")
    s3_resource.upload_file(app_event_file,bucket_name, "chohan/app_event.csv")
    print("uploaded")




    
# upload_files()
