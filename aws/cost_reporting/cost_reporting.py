import boto
import datetime
import zipfile
import os.path
import time
import csv


def getFileFromBucket():
    """Grab today's billing report from the S3 bucket, extract into pwd"""

    prefix = "794321122735-aws-billing-detailed-line-items-with-resources-and-tags-"
    csv_filename = prefix + str(datetime.date.today().isoformat()[0:7]) + ".csv"
    zip_filename = csv_filename + ".zip"

    # If local data is older than 1 day, download fresh data.
    mod_time = os.path.getmtime(csv_filename)
    if datetime.date.today() - datetime.date.fromtimestamp(mod_time) > datetime.timedelta(days=0):
        conn = boto.connect_s3()
        mybucket = conn.get_bucket('oicr.detailed.billing')
        print "Downloading " + zip_filename + "..."
        mykey = mybucket.get_key(zip_filename)
        mykey.get_contents_to_filename(zip_filename)
        print "Extracting to file " + csv_filename + "..."
        zf = zipfile.ZipFile(zip_filename)
        zf.extractall()
    return csv_filename


def read_file(sourcefile):
    """do things with the csv file
        - get all distinct names from KEEP-tags
        - ...
        ...
    """

    with open(sourcefile) as csvfile:
        reader = csv.DictReader(csvfile)

        tupled_data = []
        tupled_data_index = set()

        #print dir(reader)
        #keepers = set([])
        for row in reader:
            tupled_data.append((row['Operation'], row))
            tupled_data_index.add(row['Operation'])

        tupled_data_index = sorted(tupled_data_index)

        sorted_data = []
        for index in tupled_data_index:
            all_matches = [tup[1] for tup in tupled_data if tup[0] == index]
            for match in all_matches:
                sorted_data.append(match)

        for line in sorted_data:
            print line

            # #if row['user:KEEP'] is not "":
            # keepers.add(row['user:KEEP'])

    # return keepers


def write_untagged_report(sourcefile):
    """ Write out a TSV file that can be uploaded for others to access.
        Usage that is untagged by anyone.
    """
    with open('untagged.csv', 'w') as csvfile:
        fieldnames = ['Operation', 'Cost']
        w = csv.DictWriter(csvfile, fieldnames=fieldnames)
        w.writeheader()
        with open(sourcefile) as src:
            reader = csv.DictReader(src)
            import pdb; pdb.set_trace()
            for row in reader:
                w.writerow({'Operation': row['Operation'], 'ItemDescription': row['ItemDescription'], 'Cost': row['Cost']})
    #stub
    return ""


def write_empty_tag_report():
    """ Write out a TSV file that can be uploaded for others to access.
        Usage that is tagged but with an empty string.
    """
    #stub
    return ""


def write_users_report():
    """ Write out a TSV file that can be uploaded for others to access.
        Usage tagged by name in the KEEP-tag.
    """
    #stub
    return ""


def main():
    csv_filename = getFileFromBucket()
    keepers = read_file(csv_filename)
    # import pdb; pdb.set_trace()

    # write_untagged_report(csv_filename)
    # write_empty_tag_report()
    # write_users_report()


if __name__ == '__main__':
    main()
