import boto
import datetime
import zipfile
import os.path
import csv
from operator import itemgetter
import sys


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

    def data(self):
        # A method to return the spreadsheet in the format you want
        # Maybe it returns an iterator, or just a dictionary or a list
        # THis is the thing you'll point to whenever you need the spreadsheet
        return self.spreadsheet  # it's a list of dicts

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


def get_keepers():
    """ Returns list of strings = distinct names in the KEEP-tag """
    reader = SC.data()
    keepers = set()
    for row in reader:
        keepers.add(row['user:KEEP'])
    return list(keepers)


def subtotal(line_items):
    """ Returns subtotal for line_items.
    Used for summing costs of this particular usage type, under this Operation, PROD-tag, KEEP-tag
    """
    total_cost = 0
    for line in line_items:
        total_cost += float(line['Cost'])
    return total_cost


def process_resource(line_items):
    """Process all the line items with this particular resource ID"""
    usage_types = set([x.get('UsageType') for x in line_items])
    cost_for_this_resource = 0

    for usage_type in usage_types:
        usage_cost = subtotal([line_item for line_item in line_items if line_item['UsageType'] == usage_type])
        keeper = line_items[0].get('user:KEEP')
        if keeper == "":
            keeper = "untagged"
        with open(keeper + "_report.csv", 'a') as f:
            fields = ['user:KEEP', 'ResourceId', 'Operation', 'UsageType', 'Production?', 'Cost']
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writerow({'user:KEEP': keeper, 'ResourceId': line_items[0]['ResourceId'],
                             'Operation': line_items[0]['Operation'], 'UsageType': usage_type,
                             'Production?': line_items[0]['user:PROD'], 'Cost': usage_cost})
        cost_for_this_resource += usage_cost

    return cost_for_this_resource


def process_prod_type(line_items):
    """Process all the line items for this particular production type"""
    resources = set([x.get('ResourceId') for x in line_items])
    cost_for_this_production_type = 0
    for resource in resources:
        cost_for_this_resource = process_resource([x for x in line_items if x['ResourceId'] == resource])
        keeper = line_items[0].get('user:KEEP')
        if keeper == "":
            keeper = "untagged"
        with open(keeper + "_report.csv", 'a') as f:
            fields = ['user:KEEP', 'ResourceId', 'Operation', 'UsageType', 'Production?', 'Cost', 'subtot', 'subval']
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writerow({'subtot': "Subtotal for resource " + resource, 'subval': cost_for_this_resource})
        cost_for_this_production_type += cost_for_this_resource
    return cost_for_this_production_type


def generate_one_report(keeper):
    """Output all the subtotal info for the specified keeper"""
    line_items = [x for x in SC.spreadsheet if x['user:KEEP'] == keeper]

    prod_types = set([x.get('user:PROD') for x in line_items])  # should be just "" or "yes" but just in case

    if keeper == "":
        keeper = "untagged"
    report_name = keeper + "_report.csv"

    print "Generating report for: " + keeper + "..."

    with open(report_name, 'w') as f:
        fields = ['user:KEEP', 'ResourceId', 'Operation', 'UsageType', 'Production?', 'Cost']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({})
        writer.writerow({'user:KEEP': "Report for " + keeper + " from start of month to " + str(datetime.date.today())})
        writer.writeheader()

    cost_for_keeper = {'user:KEEP': keeper}
    # bunch all by non-production, production, or anything else in the list
    for prod_type in prod_types:
        # list of all line_items with that prod type, and process them
        cost_for_this_production_type = process_prod_type([line_item for line_item in line_items if line_item['user:PROD'] == prod_type])
        with open(report_name, 'a') as f:
            fields = ['user:KEEP', 'ResourceId', 'Operation', 'UsageType', 'Production?', 'Cost', 'subtot', 'subval']
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writerow({'subtot': "Subtotal for [non-]production:", 'subval': cost_for_this_production_type})
        cost_for_keeper[prod_type] = cost_for_this_production_type

    return cost_for_keeper


def generate_untagged_overview():
    """Give just the right amount of detail to let us know where all the untagged resources are"""
    print "Generating untagged overview report..."
    unkept = [x for x in SC.spreadsheet if x['user:KEEP'] == ""]

    with open("untagged_sorted_reports.csv", 'w') as f:

        # costs by resource
        print " ...by resource..."
        resource_ids = set([x.get('ResourceId') for x in unkept])
        fields = ['ResourceId', 'Total cost for resource']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({'ResourceId': "Untagged resources from start of month to " + str(datetime.date.today())})
        writer.writerow({})
        writer.writerow({'ResourceId': "Untagged resources, grouped by resource id"})
        writer.writeheader()
        list_of_resources = []
        for resource in resource_ids:
            resource_total = sum([float(x['Cost']) for x in unkept if x['ResourceId'] == resource])
            list_of_resources.append(dict(r=resource, c=resource_total))
        list_of_resources = sorted(list_of_resources, key=itemgetter('c'), reverse=True)
        for res in list_of_resources:
            writer.writerow({'ResourceId': res['r'], 'Total cost for resource': res['c']})

        # costs by operation
        print " ...by operation..."
        operations = set([x.get('Operation') for x in unkept])
        fields = ['Operation', 'Total cost for operation']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({})
        writer.writerow({})
        writer.writerow({'Operation': "Untagged resources, costs by Operation"})
        writer.writeheader()
        l_o_ops = []
        for op in operations:
            op_total = sum([float(x['Cost']) for x in unkept if x['Operation'] == op])
            l_o_ops.append(dict(o=op, c=op_total))
        l_o_ops = sorted(l_o_ops, key=itemgetter('c'), reverse=True)
        for oper in l_o_ops:
            writer.writerow({'Operation': oper['o'], 'Total cost for operation': oper['c']})

        # costs by usage_type
        print " ...by usage type..."
        usage_types = set([x.get('UsageType') for x in unkept])
        fields = ['UsageType', 'Total cost for UsageType']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writerow({})
        writer.writerow({})
        writer.writerow({'UsageType': "Untagged resources, costs by UsageType"})
        writer.writeheader()
        l_o_uses = []
        for usage in usage_types:
            usage_total = sum([float(x['Cost']) for x in unkept if x['UsageType'] == usage])
            l_o_uses.append(dict(u=usage, c=usage_total))
        l_o_uses = sorted(l_o_uses, key=itemgetter('c'), reverse=True)
        for use in l_o_uses:
            writer.writerow({'UsageType': use['u'], 'Total cost for UsageType': use['c']})


def generate_reports(keepers):
    """Make reports for list of keepers:
    - individual reports with every line item,
    - one report summarizing tagged,
    - one report summarizing all untagged
    """
    costs_for_keepers = []

    # Individual full reports
    for keeper in keepers:
        cost_for_keeper = generate_one_report(keeper)
        costs_for_keepers.append(cost_for_keeper)

    # Summarize
    print "Generating summary report..."
    with open('keep+prod_summary.csv', 'w') as f:
        fields = ['user:KEEP', 'non-production subtotal', 'production subtotal', 'user total']
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerow({})
        for i in range(len(keepers)):
            # ok this is not robust at all, but I'm so tired
            if 'yes' not in costs_for_keepers[i]:
                costs_for_keepers[i]['yes'] = 0
            if '' not in costs_for_keepers[i]:
                costs_for_keepers[i][''] = 0
            total = float(costs_for_keepers[i]['']) + float(costs_for_keepers[i]['yes'])
            writer.writerow({'user:KEEP': costs_for_keepers[i]['user:KEEP'],
                             'non-production subtotal': costs_for_keepers[i][''],
                             'production subtotal': costs_for_keepers[i]['yes'],
                             'user total': total})

    # Overview of untagged resources
    generate_untagged_overview()

def main():
    SC.fix_case()
    SC.sort_data()
    keepers = get_keepers()
    generate_reports(keepers)

if __name__ == '__main__':
    SC = SpreadsheetCache()
    main()
