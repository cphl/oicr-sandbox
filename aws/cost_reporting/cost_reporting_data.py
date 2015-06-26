__author__ = 'cleung'

import boto
import datetime
import zipfile
import os
import csv
from operator import itemgetter

class SpreadsheetCache(object):
    def __init__(self):
        self.filename = self.get_file_from_bucket()
        self.spreadsheet = []
        with open(self.filename) as csvfile:
            tempdata = csv.DictReader(csvfile)
            for row in tempdata:
                if float(row['Cost']) != 0 and row['RecordType'] == "LineItem":
                    if row['Operation'] == "" and row['UsageType'] == "":
                        row['Operation'] = "'ProductName = AWS Support (Developer)'"
                        row['UsageType'] = "'ProductName = AWS Support (Developer)'"
                    self.spreadsheet.append(row)
        self.keepers = set()
        for row in spreadsheet:
            keepers.add(row['user:KEEP'])
            # This is wrong, but I'm so tired.

    def data(self):
        """Returns spreadsheet (list of dicts)"""
        return self.spreadsheet

    def fix_case(self):
        # A method to operate on the spreadsheet and update the column you need uppered
        # Doesn't return anything, just fixes the spreadsheet
        for line in self.spreadsheet:
            line['user:KEEP'] = line['user:KEEP'].upper()
            line['user:PROD'] = line['user:PROD'].lower()

    def get_file_from_bucket(self):
        """Grab today's billing report from the S3 bucket, extract into pwd"""
        prefix = "794321122735-aws-billing-detailed-line-items-with-resources-and-tags-"
        csv_filename = prefix + str(datetime.date.today().isoformat()[0:7]) + ".csv"
        zip_filename = csv_filename + ".zip"
        # If local data is older than 1 day, download fresh data.
        # mod_time = os.path.getmtime(csv_filename)
        if not os.path.isfile(csv_filename) or datetime.date.today() - datetime.date.fromtimestamp(os.path.getmtime(csv_filename)) > datetime.timedelta(days=0):
            conn = boto.connect_s3()
            mybucket = conn.get_bucket('oicr.detailed.billing')
            print "Downloading " + zip_filename + "..."
            mykey = mybucket.get_key(zip_filename)
            mykey.get_contents_to_filename(zip_filename)
            print "Extracting to file " + csv_filename + "..."
            zf = zipfile.ZipFile(zip_filename)
            zf.extractall()
        return csv_filename

    def sort_data(self):
        """Sort data by KEEP, PROD, ResourceId, Operation, UsageType, Cost"""
        self.spreadsheet = sorted(self.spreadsheet, key=itemgetter('user:KEEP', 'user:PROD', 'ResourceId',
                                                                   'Operation', 'UsageType', 'Cost'))


def main():
    SC.fixcase()
    SC.sort_data()
    get_filtered_data()
    generate_reports()

if __name__ == '__main__':
    SC = SpreadsheetCache()
    main()