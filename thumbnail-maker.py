import boto3
import os
import sys
import uuid
import logging
from urllib.parse import unquote_plus
from PIL import Image

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3_client = boto3.client('s3')

def resize_image(image_path, resized_path):
  base_width = 300
  try :
    with Image.open(image_path) as image:
      w_percent = (base_width/float(image.size[0]))
      h_size = int((float(image.size[1])*float(w_percent)))
      image = image.resize((base_width,h_size), Image.ANTIALIAS)
      image.save(resized_path)

  except Exception as e:
    logger.error(f'Error resizing image {image_path} : {e}')
    raise e


def lambda_handler(event, context):
  for record in event['Records']:
    # Get the bucket name and key for the new file
    bucket = record['s3']['bucket']['name']

    # Get the object key
    key = unquote_plus(record['s3']['object']['key'])

    # Get the file name with split the key
    file_name = key.split('/')[-1]

    # Download the file from S3
    download_path = '/tmp/{}{}'.format(uuid.uuid4(), file_name)

    # Upload the resized file to S3
    upload_path = '/tmp/resized-{}'.format(file_name)

    # Download image from S3
    s3_client.download_file(bucket, key, download_path)
    
    # Resize the image
    resize_image(download_path, upload_path)

    # Upload the resized image to S3 ( upload_path mean the path of the resized image )
    s3_client.upload_file(upload_path, '{}'.format(bucket), 'thumbnail/{}'.format(file_name))
    logger.info(f'Image {file_name} resized and uploaded to /thumbnail/{file_name}')