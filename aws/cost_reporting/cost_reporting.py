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


def read_file(sourcefile):
    """do things with the csv file
        - get all untagged (right now includes empty string tags) KEEP sorted by Operation
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

        operations_totals = []
        with open('untagged.csv', 'w') as csvfile:
            # out put all my data yo
            fieldnames = ['Operation', 'Cost', 'user:KEEP', 'user:PROD', 'RecordId',
                          'RateId', 'PricingPlanId', 'UsageType', 'AvailabilityZone',
                          'ItemDescription', 'UsageQuantity', 'Rate', 'Cost', 'ResourceId']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            prev_op = sorted_data[0]['Operation']  # initialize
            op_total = 0
            for line in sorted_data:
                if line['RecordType'] == "LineItem":
                    if line['Operation'] != prev_op:
                        writer.writerow({'Operation': prev_op + ' total', 'Cost': op_total})
                        operations_totals.append({'Operation': prev_op + ' total', 'Cost': op_total})
                        op_total = 0
                        # import pdb; pdb.set_trace()
                        prev_op = line['Operation']
                    if line['user:KEEP'] == "" and line['user:PROD'] == "":

                        writer.writerow({'Operation': line['Operation'], 'Cost': line['Cost'],
                                         'user:KEEP': line['user:KEEP'], 'user:PROD': line['user:PROD'],
                                         'RecordId': line['RecordId'], 'RateId': line['RateId'],
                                         'PricingPlanId': line['PricingPlanId'], 'UsageType': line['UsageType'],
                                         'AvailabilityZone': line['AvailabilityZone'],
                                         'ItemDescription': line['ItemDescription'], 'UsageQuantity': line['UsageQuantity'],
                                         'Rate': line['Rate'], 'Cost': line['Cost'], 'ResourceId': line['ResourceId']})
                        op_total += float(line['Cost'])
            writer.writerow({'Operation': prev_op + ' total', 'Cost': op_total})            # get that last one!
            operations_totals.append({'Operation': prev_op + ' total', 'Cost': op_total})   # get that last one!

        total_untagged_cost = 0
        with open('untagged_operations_totals.csv', 'w') as total_file:
            fields = ['Operation', 'Cost']
            w = csv.DictWriter(total_file, fieldnames=fields)
            w.writeheader()
            for row in operations_totals:
                w.writerow({'Operation': row['Operation'], 'Cost': row['Cost']})
                total_untagged_cost += row['Cost']
            w.writerow({'Operation': 'Overall untagged operations total', 'Cost': total_untagged_cost})

            # if row['user:KEEP'] is not "":
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
    read_file(csv_filename)
    # import pdb; pdb.set_trace()

    # write_untagged_report(csv_filename)
    # write_empty_tag_report()
    # write_users_report()


if __name__ == '__main__':
    main()
