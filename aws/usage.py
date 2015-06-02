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

# snapshots got this thing where there are public, private, and owned by me
# we're interested in the ones owned by us, so select those with 'owner_id' = 794321122735
# I think I can use 'self' as a parameter to get_all_snapshots() too
def getSnapshots(region):
    creds = credentials()
    try:
        conn = ec2.connect_to_region(region, **creds)
        snapshots = conn.get_all_snapshots(owner='self')
    except boto.exception.EC2ResponseError:
        return []
    return snapshots

def getKeepTag(obj):
    try:
        tag = obj.tags['KEEP']
    except:
        #TODO: double-check with web console that empty strings and non-existent KEEP-tags match
        return "no tag with key 'KEEP'"
    return tag

def getGroups(instance):
    groupList = []
    for g in instance.groups:
        #TODO: make output prettier? It's just stringifying a list right now.
        groupList.append(g.name)
    return groupList

#TODO: add time-passed since launch (in lieu of time tagless)

#TODO: write directly to file



def main ():
    regions = getRegions()

    # Print volumes
    print "\n+ VOLUMES +"
    print "volume_ID\tstate\tsize\ttime_stamp_volume_created\tregion(zone)\tsnapshot ID"
    for r in regions:
        volumes = getVolumes(r)
        for v in volumes:
            print "%s\t%s\t%s GB\t%s\t%s\t%s" % (v.id, v.status, v.size, v.create_time, v.zone, v.snapshot_id)

    # Snapshots
    print "\n+ SNAPSHOTS +"
    snapshots = []
#    import pdb; pdb.set_trace()
    for r in regions:
        snapshots += getSnapshots(r)

    print "snapshot_id\tstatus\tregion(availability_zone)\tprogress\tstart_date_time_stamp\tvolume_id\tvolume_size\tKEEP-tag"

    for s in snapshots:
           print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (s.id, s.status, s.region, s.progress, s.start_time, s.volume_id, s.volume_size, getKeepTag(s))

    # Instances
    print "\n+ INSTANCES +"
    print "instance ID\tinstance_type\tstate\ttime_stamp_instance_launched\tsecurity_groups(list?)\tKEEP-tag"
    for region in regions:
        instances = getInstances(region)
        for i in instances:
            print "%s\t%s\t%s\t%s\t%s\t%s" % (i.id, i.instance_type, i.state, i.launch_time, getGroups(i), getKeepTag(i))


if __name__ == '__main__':
    main()