import boto
import datetime
import zipfile
import os.path
import time

def getFileFromBucket():
    """Grab today's billing report from the S3 bucket, extract into pwd"""
    conn = boto.connect_s3()
    mybucket = conn.get_bucket('oicr.detailed.billing')

    prefix = "794321122735-aws-billing-detailed-line-items-with-resources-and-tags-"
    csv_filename = prefix + str(datetime.date.today().isoformat()[0:7]) + ".csv"
    zip_filename = csv_filename + ".zip"
    import pdb; pdb.set_trace()

    # arrrgh file name only goes up to month granularity
    # check file modified time

    # See if we have the unzipped .csv file
    if not os.path.isfile(csv_filename):
        # See if we have the .zip file
        if not os.path.isfile(zip_filename):
            print "Downloading " + zip_filename + "..."
            mykey = mybucket.get_key(zip_filename)
            mykey.get_contents_to_filename(zip_filename)
        print "Extracting to file " + csv_filename + "..."
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

if __name__ == '__main__':
    main()
