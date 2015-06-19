import boto
import datetime
import zipfile
import os.path
import time
import csv
from operator import itemgetter
import sys

untagged_full_report = 'untagged.csv'
untagged_summary_report = 'untagged_operations_totals.csv'
tagged_full_report = 'tagged.csv'
tagged_summary_report = 'tagged_user_totals.csv'


class SpreadsheetCache(object):
    def __init__(self):
        self.filename = self.get_file_from_bucket()
        self.spreadsheet = []
        with open(self.filename) as csvfile:
            tempdata = csv.DictReader(csvfile)
            for row in tempdata:
                self.spreadsheet.append(row)

    def data(self):
        # A method to return the spreadsheet in the format you want
        # Maybe it returns an iterator, or just a dictionary or a list
        # THis is the thing you'll point to whenever you need the spreadsheet
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


def process_untagged():
    """ Write out TSV files that can be uploaded for others to access:
            - One has all untagged resources sorted by Operation and totalled.
            - Other has only a summary of totals with Operation and Cost.
        Usage that is either untagged or tagged with an empty string (indistinguishable in the cost report)
    """
    reader = SC.data()
    tupled_data = []  # list of (op, whole row)
    tupled_data_index = set()

    for row in reader:
        tupled_data.append((row['Operation'], row))
        tupled_data_index.add(row['Operation'])

    tupled_data_index = sorted(tupled_data_index)

    sorted_data = []
    for index in tupled_data_index:
        all_matches = [tup[1] for tup in tupled_data if tup[0] == index]
        for match in all_matches:
            sorted_data.append(match)

    operations_totals = []
    print "Generating report for untagged resources: " + untagged_full_report + "..."
    with open(untagged_full_report, 'w') as csvfile:
        # out put all my data yo
        fieldnames = ['Operation', 'Cost', 'user:KEEP', 'user:PROD', 'RecordId', 'RateId', 'PricingPlanId',
                      'UsageType', 'AvailabilityZone', 'ItemDescription', 'UsageQuantity', 'Rate', 'Cost',
                      'ResourceId']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        prev_op = sorted_data[0]['Operation']  # initialize
        op_total = 0
        for line in sorted_data:
            if line['RecordType'] == "LineItem":

                # Select all the untagged, INCLUDING those that are tagged but with an empty string
                if line['user:KEEP'] == "":  # and line['user:PROD'] == "":
                    if line['Operation'] != prev_op:
                        writer.writerow({'Operation': prev_op + ' total', 'Cost': op_total})
                        operations_totals.append({'Operation': '"' + prev_op + '" total', 'Cost': op_total})
                        op_total = 0
                        # import pdb; pdb.set_trace()
                        prev_op = line['Operation']
                    writer.writerow({'Operation': line['Operation'], 'Cost': line['Cost'],
                                     'user:KEEP': line['user:KEEP'], 'user:PROD': line['user:PROD'],
                                     'RecordId': line['RecordId'], 'RateId': line['RateId'],
                                     'PricingPlanId': line['PricingPlanId'], 'UsageType': line['UsageType'],
                                     'AvailabilityZone': line['AvailabilityZone'],
                                     'ItemDescription': line['ItemDescription'],
                                     'UsageQuantity': line['UsageQuantity'], 'Rate': line['Rate'],
                                     'Cost': line['Cost'], 'ResourceId': line['ResourceId']})
                    op_total += float(line['Cost'])
        writer.writerow({'Operation': prev_op + ' total', 'Cost': op_total})                    # get that last one!
        operations_totals.append({'Operation': '"' + prev_op + '" total', 'Cost': op_total})    # get that last one!

    print "Generating summary of totals for untagged resources: " + untagged_summary_report
    total_untagged_cost = 0
    with open(untagged_summary_report, 'w') as total_file:
        fields = ['Operation', 'Cost']
        w = csv.DictWriter(total_file, fieldnames=fields)
        w.writeheader()
        for row in operations_totals:
            w.writerow({'Operation': row['Operation'], 'Cost': row['Cost']})
            total_untagged_cost += row['Cost']
        w.writerow({'Operation': 'Overall untagged operations total', 'Cost': total_untagged_cost})


def process_tagged():
    """ Write out a TSV file that can be uploaded for others to access.
        Usage tagged by name in the KEEP-tag.
    """

    # Get data and sort it by KEEP and PROD tags
    reader = SC.data()
    keep_tagged_data = []
    for row in reader:
        if row['user:KEEP'] != "":
            keep_tagged_data.append(row)
    keep_tagged_data = sorted(keep_tagged_data, key=itemgetter('user:KEEP', 'user:PROD', 'Operation'))

    print "Generating report of tagged resources: " + tagged_full_report
    with open(tagged_full_report, 'w') as f:
        fields = ['user:KEEP', 'user:PROD', 'Operation', 'RecordId', 'RateId', 'PricingPlanId', 'UsageType',
                  'AvailabilityZone', 'ItemDescription', 'UsageQuantity', 'Rate', 'Cost', 'ResourceId']
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerow({})

        # accounting
        keeper_sum = 0
        keeper_sums = []
        prev_keeper = keep_tagged_data[0]['user:KEEP']  # first one might be crap
        non_prod_sum = 0
        prod_sum = 0
        non_prod_sums = []
        prod_sums = []

        # TODO: final total dump out

        for row in keep_tagged_data:

            # If we're at a new keeper, dump the info about the last keeper
            if prev_keeper != row['user:KEEP']:
                if True:  # non-production subtotal for this keeper
                    w.writerow({'Rate': 'non-production total', 'Cost': non_prod_sum})
                    non_prod_sums.append(non_prod_sum)
                    non_prod_sum = 0

                if True:  # production total for this keeper
                    w.writerow({'Rate': 'production total', 'Cost': prod_sum})
                    prod_sums.append(prod_sum)
                    prod_sum = 0

                w.writerow({'Rate': 'Total for "' + prev_keeper + '" in KEEP tag', 'Cost': keeper_sum})
                w.writerow({})
                keeper_sums.append({'user:KEEP': prev_keeper, 'Cost': keeper_sum})
                keeper_sum = 0
                prev_keeper = row['user:KEEP']

            # Subtotals for each keeper's production or non-production resources
            if row['user:PROD'] == "":
                non_prod_sum += float(row['Cost'])
            else:
                prod_sum += float(row['Cost'])

            # Deal with the individual row
            w.writerow({'user:KEEP': row['user:KEEP'], 'user:PROD': row['user:PROD'], 'Operation': row['Operation'],
                        'RecordId': row['RecordId'], 'RateId': row['RateId'], 'PricingPlanId': row['PricingPlanId'],
                        'UsageType': row['UsageType'], 'AvailabilityZone': row['AvailabilityZone'],
                        'ItemDescription': row['ItemDescription'], 'UsageQuantity': row['UsageQuantity'],
                        'Rate': row['Rate'], 'Cost': row['Cost'], 'ResourceId': row['ResourceId']})

            # do summing stuff
            keeper_sum += float(row['Cost'])

        # Cheat: final dump for last member, fix later
        w.writerow({})
        w.writerow({'Rate': 'non-production total', 'Cost': non_prod_sum})
        w.writerow({'Rate': 'production total', 'Cost': prod_sum})
        w.writerow({'Rate': 'Total for "' + prev_keeper + '" in KEEP tag', 'Cost': keeper_sum})
        non_prod_sums.append(non_prod_sum)
        prod_sums.append(prod_sum)
        keeper_sums.append({'user:KEEP': prev_keeper, 'Cost': keeper_sum})

    print "Generating summary of tagged resources, costs by KEEP-tag: " + tagged_summary_report
    with open(tagged_summary_report, 'w') as f2:
        fields2 = ['KEEP-tag entry', 'Cost']
        w2 = csv.DictWriter(f2, fieldnames=fields2)
        w2.writeheader()
        # can't think, fix later
        for i in range(len(keeper_sums)):
            w2.writerow({})
            w2.writerow({'KEEP-tag entry': keeper_sums[i]['user:KEEP'] + ' non-production', 'Cost': non_prod_sums[i]})
            w2.writerow({'KEEP-tag entry': keeper_sums[i]['user:KEEP'] + ' production', 'Cost': prod_sums[i]})
            w2.writerow({'KEEP-tag entry': keeper_sums[i]['user:KEEP'] + ' total', 'Cost': keeper_sums[i]['Cost']})


def get_keepers():
    """ Returns set of strings = names in the KEEP-tag """
    reader = SC.data()
    keepers = set()
    for row in reader:
        keepers.add(row['user:KEEP'])
    return keepers


def generate_one_report():
    """Sort and tabulate totals for any given KEEP-tag, including the blank ones"""
    pass


def main():
    SC.fix_case()
    keepers = get_keepers()
    # process_untagged()
    # process_tagged()


if __name__ == '__main__':
    SC = SpreadsheetCache()
    main()
