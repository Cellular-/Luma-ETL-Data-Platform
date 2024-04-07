import boto3, os, logging, definitions as defs
from botocore.exceptions import ClientError

TEMP_DIR = os.path.join(defs.ROOT_DIR, 'tmp')

def upload_file(file_name, bucket, object_name=None) -> bool:
    """
    Upload a file to an S3 bucket. Returns True if upload was
    successful else False.

    file_name   -- File to upload
    bucket      -- Bucket to upload to
    object_name -- S3 object name. If not specified then file_name is used
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    try:
        response = bucket.upload_file(file_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def get_object(bucket, subfolder, source_file_name, desination_file_name='') -> bool:
    """
    Downloads an object from an S3 bucket into the destination folder/file.

    bucket                -- name of S3 bucket
    subfolder             -- prepended to filename in case object is in subfolder(s)
    source_file_name      -- name of object 
    destination_file_name -- target to store the object
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)

    try:
        response = bucket.download_file(
            '/'.join([subfolder, source_file_name]) if subfolder else source_file_name, 
            desination_file_name or os.path.join(TEMP_DIR, source_file_name)
        )
    except ClientError as e:
        logging.error(e)
        return False
    
    return True

def put_object(bucket, name):
    """
    Places an object with `name` into the bucket.

    bucket                -- name of S3 bucket
    name                  -- name of object to put in bucket
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)

    try:
        response = bucket.put_object(Key=name)
    except ClientError as e:
        logging.error(e)
        return False
    
    return True
