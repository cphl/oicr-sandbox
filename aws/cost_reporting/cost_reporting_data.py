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
                        row['Operation'] = "ProductName" + row['ProductName']
                        row['UsageType'] = "ProductName" + row['ProductName']
                    self.spreadsheet.append(row)
        self.fix_case()
        self.sort_data()
        self.keepers = set()
        for row in self.spreadsheet:
            self.keepers.add(row['user:KEEP'])

        # initial value for list of untagged items, will have items removed as tagged lists are populated
        self.unkept = self.spreadsheet

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

    def get_resources_for_one_keeper(self, keeper):
        """ Includes resources untagged earlier, if they are tagged on the day of billing data download.
        Alters running list of untagged resource line items.
        :param keeper: String representing the person associated with the resources
        :return: List of dicts, each dict is one line item belonging to the keeper
        """
        line_items_of_keeper = []
        resource_ids = set()
        for row in self.spreadsheet:
            if row['user:KEEP'] == keeper:
                resource_ids.add(row['ResourceId'])
                print "."
        # Now (go back in time and) grab all the line items with these resource IDs even if untagged in past
        for row in self.spreadsheet:
            if row['ResourceId'] in resource_ids:
                print "-"
                line_items_of_keeper.append(row)
                self.unkept.remove(row)
        return line_items_of_keeper

    # def get_resources_for_keepers(self):
    #     """ Iterate through list of keepers to populate resource lists for all
    #     :return: list of list of dicts (i.e. list of {keeper: list of their resources})
    #     """
    #     self.keepers_data = []  # list of dicts: keepers with their resources
    #         for row in self.spreadsheet:
    #             if row['user:KEEP'] == "":
    #                 keeper = 'untagged'
    #             else:
    #                 keeper = row['user:KEEP']
    #             # This won't work, I don't think. Also I need this to do something else. Abandon ship.
    #             self.keepers_data.append(dict(keeper=keeper, list_of_resources=list_of_resources.append{row}))

def get_resources_for_keepers():
    """populate keepers' line item lists, depopulate unkept list (starts as full)
    :return: dict of dicts: key = keeper, value = list of dicts, each dict represents a line item
    """
    keepers_line_items = {}
    for keeper in SC.keepers:
        if keeper != "":
            print "Getting line items for " + keeper
            keepers_line_items[keeper] = SC.get_resources_for_one_keeper(keeper)
    return keepers_line_items


def main():
    keepers_line_items = get_resources_for_keepers()
    kli0 = keepers_line_items[0]
    import pdb; pdb.set_trace()
    # get_filtered_data()
    # generate_reports()

if __name__ == '__main__':
    SC = SpreadsheetCache()
    main()