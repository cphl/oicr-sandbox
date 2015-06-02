# Well this is basically all Niall's stuff from before
import os
import sys
import boto
from boto import ec2

def credentials():
    return {"aws_access_key_id": os.environ['AWS_ACCESS_KEY'],
            "aws_secret_access_key": os.environ['AWS_SECRET_KEY']}

def getInstances(region):
    creds = credentials()
    try:
        conn = ec2.connect_to_region(region, **creds)
        instances = []
        reservations = conn.get_all_reservations()
        for reservation in reservations:
            for instance in reservation.instances:
                instances.append(instance)
    except boto.exception.EC2ResponseError:
        return []
    return instances

def getVolumes(region):
    creds = credentials()
    try:
        conn = ec2.connect_to_region(region, **creds)
        volumes = conn.get_all_volumes()
    except boto.exception.EC2ResponseError:
        return []
    return volumes

def getRegions():
    regions = ec2.regions()
    region_names = []
    for region in regions:
        region_names.append(region.name)
    return region_names

def getTag(instance):
    try:
        tag = instance.tags['KEEP']
    except:
        #TODO: double-check with web console that empty strings and non-existent KEEP-tags match
        return "no tag with key 'KEEP'"
    return tag

def getGroups(instance):
    groupList = []
    for g in instance.groups:
        #TODO: make output prettier?
        groupList.append(g.name)
    return groupList

def main ():
    regions = getRegions()

    # Volumes
#    print "\nVolumes volumes volumes!!!!"
#    print "volume_ID\tstate\tsize\ttime_stamp_volume_created\tzone\tsnapshot ID"
#    for r in regions:
#        volumes = getVolumes(r)
#        for v in volumes:
#            print "%s\t%s\t%s GB\t%s\t%s\t%s" % (v.id, v.status, v.size, v.create_time, v.zone, v.snapshot_id)

    # Snapshots
    print "\nSnapshots: What's a snapshot? I...what?"

    # Instances
    print "\nInstances!!!!!"
    print "instance ID\tinstance_type\tstate\ttime_stamp_instance_launched\tgroups\tKEEP-tag"
    for region in regions:
        instances = getInstances(region)
        for i in instances:

            # do something for every instance we have
            # groups is a list of things where you gotta do element.name


            print "%s\t%s\t%s\t%s\t%s\t%s" % (i.id, i.instance_type, i.state, i.launch_time, getGroups(i), getTag(i))


if __name__ == '__main__':
    main()
