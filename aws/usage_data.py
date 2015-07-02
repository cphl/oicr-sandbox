__author__ = 'cleung'
# Generate reports showing AWS snapshots, AMIs, volumes, and instances; and KEEP-tags, PROD-tags
# Snapshots show associated AMIs and KEEP-tags thereof
# Volumes show associated instances and the KEEP-tags thereof
# Code borrowed heavily from Niall's previous script: volume_cleanup.py

import os
import sys
import boto
from boto import ec2

# Name your output files
volumes_data_output_file = "volumes.tsv"
snapshots_data_output_file = "snapshots.tsv"
instances_data_output_file = "instances.tsv"
images_data_output_file = "images.tsv"

class Resource(object):
    def __init__(self, res_type):
        self.res_type = res_type
        self.spreadsheet = []
        # populate depending on type
        if self.res_type == "volume":
            self.populate_volumes()
        # elif self.res_type == "snapshots":
        #     populate_snapshots(self)
        elif self.res_type == "instance":
            self.populate_instances()
        # elif self.res_type == "image":
        #     populate_images(self)

    @staticmethod
    def get_regions():
        regions = ec2.regions()
        region_names = []
        for region in regions:
            region_names.append(region.name)
        return region_names

    @staticmethod
    def get_volumes(region):
        """Return list of whole volumes for a given region"""
        credentials = get_credentials()
        try:
            conn = ec2.connect_to_region(region, **credentials)
            region_volumes = conn.get_all_volumes()
        except boto.exception.EC2ResponseError:
            return []  # This better not fail silently or I'll cut a person.
        return region_volumes

    @staticmethod
    def get_all_volumes():
        regions = Resource.get_regions()
        all_volumes = []
        for region in regions:
            all_volumes.extend(Resource.get_volumes(region))
        return all_volumes

    @staticmethod
    def get_instances(region):
        """Return list of whole instances for given region"""
        credentials = get_credentials()
        try:
            conn = ec2.connect_to_region(region, **credentials)
            region_instances = []
            reservations = conn.get_all_reservations()
            for reservation in reservations:
                for instance in reservation.instances:
                    region_instances.append(instance)
        except boto.exception.EC2ResponseError:
            return []
        return region_instances

    @staticmethod
    def get_all_instances():
        regions = Resource.get_regions()
        all_instances = []
        for region in regions:
            all_instances.extend(Resource.get_instances(region))
        return all_instances

    @staticmethod
    def get_name_tag(obj):
        """ 'Name' is an optional tag. Get it if it exists."""
        if 'Name' in obj.tags:
            return obj.tags['Name']
        else:
            return ""

    @staticmethod
    def get_keep_tag(obj):
        """Get the KEEP tag, if it exists. Empty strings count as untagged in this version."""
        if 'KEEP' in obj.tags and len(obj.tags['KEEP'].strip()) != 0:
            return obj.tags['KEEP']
        else:
            return "-------no-tag"

    @staticmethod
    def is_production(obj):
        return 'PROD' in obj.tags

    def populate_volumes(self):
        """Make a list of dictionaries representing volumes."""
        volumes = Resource.get_all_volumes()
        for i in volumes:
            import pdb; pdb.set_trace()

            # handle associated instance's KEEP-tag
            associated_instance_id = i.attach_data.instance_id.encode()
            instance_keep_tag = (j for j in Ins.spreadsheet if j["id"] == associated_instance_id).next()['KEEP_tag']
            # oh heck yes!
            self.spreadsheet.append(
                dict(Name_tag=Resource.get_name_tag(i), id=i.id, KEEP_tag=Resource.get_keep_tag(i),
                     instance_KEEP_tag=instance_keep_tag, associated_instance_id=associated_instance_id,
                     PROD_tag=Resource.is_production(i),
                     ##### TODO: Start checking values from here ###

                     instance_type=i.instance_type, state=i.state,
                     launched=i.launch_time, region=i.region.name))

    def populate_instances(self):
        """Make a list of dictionaries with the all fields we want"""
        instances = Resource.get_all_instances()
        for i in instances:
            self.spreadsheet.append(
                dict(Name_tag=Resource.get_name_tag(i), id=i.id, KEEP_tag=Resource.get_keep_tag(i),
                     PROD_tag=Resource.is_production(i), instance_type=i.instance_type, state=i.state,
                     launched=i.launch_time, region=i.region.name))


def get_credentials():
    return {"aws_access_key_id": os.environ['AWS_ACCESS_KEY'],
            "aws_secret_access_key": os.environ['AWS_SECRET_KEY']}



def generate_reports():
    generate_report(Vols.spreadsheet)
    generate_reports(Snaps.spreadsheet)
    generate_reports(Ins.spreadsheet)
    generate_reports(Ims.spreadsheet)


def main():
    all_instances = Resource.get_all_instances()
    import pdb; pdb.set_trace()
    # generate_reports()


if __name__ == '__main__':
    Ins = Resource('instance')
    Vols = Resource('volume')
    # Snaps = Resource('snapshot')
    # Ims = Resource('image')
    main()