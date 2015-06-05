# this is the example code given at http://aws.amazon.com/developers/getting-started/python/

import boto
import uuid

# instantiate new client for S3, uses creds in ~/.aws/credentials (via environment variables)
s3_client = boto.connect_s3()

# uploads to S3 must be to a bucket, which must have a unique name
bucket_name = "keep-tag_report-%s" % uuid.uuid4()
print "The reports will be in bucket: " + bucket_name
my_shiny_new_bucket = s3_client.create_bucket(bucket_name)
print "Created bucket..."

# Object (a.k.a. files): Refer to it by its key (name)
from boto.s3.key import Key
key_aka_nameOfObject = Key(my_shiny_new_bucket)
key_aka_nameOfObject.key = 'volume.tsv'

print "Uploading data to " + bucket_name + " with key: " + key_aka_nameOfObject.key

# Put a bit of data into the object a.k.a. file
key_aka_nameOfObject.set_contents_from_filename("test.tsv")


# use generate_url to make an URL
seconds_to_expire = 240  # you have 4 minutes

print "Making a public URL for the uploaded object. Lives for %d seconds." % seconds_to_expire
print
print key_aka_nameOfObject.generate_url(seconds_to_expire)
print
raw_input("Press enter to delete object and bucket...")

# Need to delete object first because bucket has to be empty before it can be deleted
print "Deleting object..."
key_aka_nameOfObject.delete()

# Bucket is emptied, so we can deleted it
print "Deleting bucket " + bucket_name
s3_client.delete_bucket(bucket_name)
