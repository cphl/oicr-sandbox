import boto
import datetime
import zipfile
import os.path
import time

def getFileFromBucket():
    """Grab today's billing report from the S3 bucket, extract into pwd"""

    prefix = "794321122735-aws-billing-detailed-line-items-with-resources-and-tags-"
    csv_filename = prefix + str(datetime.date.today().isoformat()[0:7]) + ".csv"
    zip_filename = csv_filename + ".zip"

    # If local data is older than 1 day, download fresh data.
    mod_time = os.path.getmtime(csv_filename)
    if datetime.date.today() - datetime.date.fromtimestamp(mod_time) > datetime.timedelta(days=1):
        conn = boto.connect_s3()
        mybucket = conn.get_bucket('oicr.detailed.billing')
        print "Downloading " + zip_filename + "..."
        mykey = mybucket.get_key(zip_filename)
        mykey.get_contents_to_filename(zip_filename)
        print "Extracting to file " + csv_filename + "..."
        zf = zipfile.ZipFile(zip_filename)
        zf.extractall()


def getPeople():
    """Return all the different names in the KEEP-tags"""
    people = []
    import pdb; pdb.set_trace()
    #stub
    return people


def main():
    getFileFromBucket()
    list_of_peepz = getPeople()

if __name__ == '__main__':
    main()
