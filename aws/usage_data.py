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
        self.spreadsheet = {}

        Resource.region_names = []  # dunno why "Resource" has to be there
        self.get_regions()

        Resource.credentials = self.get_credentials()

        # populate depending on type
        if self.res_type == "instance":
            self.populate_instances()
        elif self.res_type == "snapshot":
            self.populate_snapshots()
        elif self.res_type == "volume":
            self.populate_volumes()
        elif self.res_type == "image":
            self.populate_images()

    @staticmethod
    def get_credentials():
        return {"aws_access_key_id": os.environ['AWS_ACCESS_KEY'],
                "aws_secret_access_key": os.environ['AWS_SECRET_KEY']}

    @staticmethod
    def get_regions():
        regions = ec2.regions()
        for region in regions:
            Resource.region_names.append(region.name)

    @staticmethod
    def get_volumes(region):
        """Return list of whole volumes for a given region"""
        try:
            conn = ec2.connect_to_region(region, **Resource.credentials)
            region_volumes = conn.get_all_volumes()
        except boto.exception.EC2ResponseError:
            return []  # This better not fail silently or I'll cut a person.
        return region_volumes

    @staticmethod
    def get_all_volumes():
        all_volumes = []
        for region in Resource.region_names:
            all_volumes.extend(Resource.get_volumes(region))
        return all_volumes

    @staticmethod
    def get_instances(region):
        """Return list of whole instances for given region"""
        try:
            conn = ec2.connect_to_region(region, **Resource.credentials)
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
        all_instances = []
        for region in Resource.region_names:
            all_instances.extend(Resource.get_instances(region))
        return all_instances

    @staticmethod
    def get_snapshots(region):
        """Return list of whole snapshots for a given region"""
        try:
            conn = ec2.connect_to_region(region, **Resource.credentials)
            region_snapshots = conn.get_all_snapshots(owner='self')
        except boto.exception.EC2ResponseError:
            return []
        return region_snapshots

    @staticmethod
    def get_all_snapshots():
        all_snapshots = []
        for region in Resource.region_names:
            all_snapshots.extend(Resource.get_snapshots(region))
        return all_snapshots

    @staticmethod
    def get_name_tag(obj):
        """ 'Name' is an optional tag. Get it if it exists."""
        if 'Name' in obj.tags:
            return obj.tags['Name']
        else:
            return ""

    @staticmethod
    def get_keep_tag(obj):
        """Get the KEEP tag from source, if it exists. Empty strings count as untagged in this version."""
        if 'KEEP' in obj.tags and len(obj.tags['KEEP'].strip()) != 0:
            return obj.tags['KEEP']
        else:
            return "-------no-tag"

    @staticmethod
    def is_production(obj):
        return 'PROD' in obj.tags

    @staticmethod
    def get_amis_of(snapshot_id):
        """Get the AMI ids associated with a given snapshot"""
        mes_amis = []
        # There has GOT to be a better way. Hmm... maybe not
        keys = Ims.spreadsheet.keys()
        for key in keys:
            if snapshot_id in Ims.spreadsheet[key]['associated_snapshots']:
                mes_amis.append(key)
        return mes_amis

    @staticmethod
    def get_snapshots_of(image):
        """Return the snapshot ids (strings) associated with this AMI"""
        snapshot_ids = []
        device_mapping = image.block_device_mapping  # dict of devices
        devices = device_mapping.keys()
        for device in devices:
            if device_mapping[device].snapshot_id is not None:
                snapshot_ids.append(device_mapping[device].snapshot_id.encode())  # do I need to have 'encode' here?
        return snapshot_ids

    @staticmethod
    def get_images(region):
        """Get whole AMIs for a given region"""
        try:
            conn = ec2.connect_to_region(region, **Resource.credentials)
            region_images = conn.get_all_images(owners=['self'])
        except boto.exception.EC2ResponseError:
            return []
        return region_images

    @staticmethod
    def get_all_images():
        all_images = []
        for region in Resource.region_names:
            all_images.extend(Resource.get_images(region))
        return all_images

    def populate_images(self):
        """Dict of dicts for images"""
        print "Populating images dictionary..."
        images = Resource.get_all_images()
        for i in images:

            associated_snapshots = Resource.get_snapshots_of(i)

            self.spreadsheet[i.id] = dict(name=i.name, Name_tag=Resource.get_name_tag(i), id=i.id,
                                          KEEP_tag=Resource.get_keep_tag(i), PROD_tag=Resource.is_production(i),
                                          region=i.region.name,
                                          created=i.creationDate,
                                          associated_snapshots=associated_snapshots,
                                          description=i.description)

    def populate_volumes(self):
        """Dictionary of dictionaries representing volumes"""
        print "Populating volumes dictionary..."
        volumes = Resource.get_all_volumes()
        for i in volumes:

            # handle associated instance's KEEP-tag
            associated_instance_id = i.attach_data.instance_id

            # instance_keep_tag = (j for j in Ins.spreadsheet if j["id"] == associated_instance_id).next()['KEEP_tag']
            # TODO: only kinda works but maybe better if I did a look-up on a dict of dicts, could timeit later.
            # TODO: would be nice to figure out what the error means: StopIteration exception

            if associated_instance_id is None:  # sometimes there is no attached instance
                instance_keep_tag = "-------no-instance-found"
            else:
                instance_keep_tag = Ins.spreadsheet[associated_instance_id]['KEEP_tag']
            self.spreadsheet[i.id] = dict(Name_tag=Resource.get_name_tag(i), id=i.id, KEEP_tag=Resource.get_keep_tag(i),
                                          instance_KEEP_tag=instance_keep_tag,
                                          associated_instance_id=associated_instance_id,
                                          PROD_tag=Resource.is_production(i), attachment_state=i.attachment_state(),
                                          state=i.volume_state(), status=i.status, iops=i.iops, size=i.size,
                                          created=i.create_time, region=i.region.name)

    def populate_instances(self):
        """Make a dictionary of dictionaries with the all fields we want
        Dict is nice so that we can easily look up instance KEEP-tags later.
        """
        print "Populating instances dictionary..."
        instances = Resource.get_all_instances()
        for i in instances:
            self.spreadsheet[i.id] = dict(Name_tag=Resource.get_name_tag(i), id=i.id, KEEP_tag=Resource.get_keep_tag(i),
                                          PROD_tag=Resource.is_production(i), instance_type=i.instance_type,
                                          state=i.state, launched=i.launch_time, region=i.region.name)

    def populate_snapshots(self):
        """Dict of dicts for snapshots"""
        print "Populating snapshots dictionary..."
        snapshots = Resource.get_all_snapshots()

        for i in snapshots:

            # find the ami id(s) for this snapshot. API allows for multiple even though I don't think there would be
            associated_ami_ids = Resource.get_amis_of(i.id)

            ami_keep_tags = [Ims.spreadsheet[ami_id]['KEEP_tag'] for ami_id in associated_ami_ids]

            # deal with none, single, or multi AMIs and their respective KEEP-tags, if existent
            # TODO: this should be taken care of during output, not here. :( oh well! :D fix later!
            if len(associated_ami_ids) == 1:
                associated_ami_ids = associated_ami_ids[0]
                ami_keep_tags = ami_keep_tags[0]
            elif len(associated_ami_ids) == 0:
                associated_ami_ids = "-------no-AMI-found"
                ami_keep_tags = "-------no-AMI-found"
            else:
                amis = ""
                keep_tags = ""
                for ami_ids in associated_ami_ids:
                    amis += ami_ids + " "
                for keeps in ami_keep_tags:
                    keep_tags += keeps + " "

            self.spreadsheet[i.id] = dict(Name_tag=Resource.get_name_tag(i), id=i.id, KEEP_tag=Resource.get_keep_tag(i),
                                          ami_KEEP_tag=ami_keep_tags, associated_ami_ids=associated_ami_ids,
                                          PROD_tag=Resource.is_production(i), start_time=i.start_time,
                                          region=i.region.name, associated_volume=i.volume_id,
                                          volume_size=i.volume_size, description=i.description)


def generate_reports():
    generate_report(Vols.spreadsheet)
    generate_report(Snaps.spreadsheet)
    generate_report(Ins.spreadsheet)
    generate_report(Ims.spreadsheet)


def main():
    import pdb; pdb.set_trace()
    # generate_reports()
    print "done"

if __name__ == '__main__':
    Ins = Resource('instance')
    Vols = Resource('volume')
    Ims = Resource('image')
    Snaps = Resource('snapshot')
    main()
