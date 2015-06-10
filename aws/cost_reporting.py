import boto
from datetime import date
import zipfile


def getFileFromBucket():
    """Grab today's billing report from the S3 bucket, extract into pwd"""
    conn = boto.connect_s3()
    mybucket = conn.get_bucket('oicr.detailed.billing')

    prefix = "794321122735-aws-billing-detailed-line-items-with-resources-and-tags-"
    suffix = ".csv.zip"
    filename_string = prefix + str(date.today().isoformat()[0:7]) + suffix

    mykey = mybucket.get_key(filename_string)
    mykey.get_contents_to_filename(filename_string)
    zf = zipfile.ZipFile(filename_string)
    zf.extractall()


def getPeople():
    """Return all the different names in the KEEP-tags"""
    people = []
    #stub
    return people


def main():
    getFileFromBucket()
    getPeople()


if __name__ == '__main()__':
    main()
