# Well this is basically all Niall's stuff from before
import os
import sys
import boto
from boto import ec2

volumes_data_output_file = "volumes.tsv"
snapshots_data_output_file = "snapshots.tsv"
instances_data_output_file = "instances.tsv"
images_data_output_file = "images.tsv"

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
# can use owner='self' as a parameter to get_all_snapshots() too
def getSnapshots(region):
    creds = credentials()
    try:
        conn = ec2.connect_to_region(region, **creds)
        snapshots = conn.get_all_snapshots(owner='self')
    except boto.exception.EC2ResponseError:
        return []
    return snapshots

"""Return images for one given region, owned by self"""
def getImages(region):
    creds = credentials()
    try:
        conn = ec2.connect_to_region(region, **creds)
        images = conn.get_all_images(owners='self')
    except boto.exception.EC2ResponseError:
        return []
    return images

"""Use dictionaries 'cos we'll have to cross-reference to get snapshots that go with the AMIs"""
def getImagesD(region):
    creds = credentials()
    try:
        conn = ec2.connect_to_region(region)
        images = conn.get_all_images(owners='self')
    except boto.exception.EC2ResponseError:
        return []

    imageDicts = []
    for im in images:
        imageDict = {"name":        im.name,
                     "id":          im.id,
                     "region":      im.region.name,
                     "state":       im.state,
                     "created":     im.creationDate,
                     "type":        im.type,
                     "KEEP":        getKeepTag(im),
                     "description": im.description
        }
        imageDicts.append(imageDict)
    return imageDicts
###########################################################!!!!!!!!!!!############################
###################################### TEST THIS !!!!!!!!!!!!!!!!!!!!!############################
##################################################################################################

def getKeepTag(obj):
    try:
        tag = obj.tags['KEEP']
    except:
        # Note: some with empty KEEP-tags, through web console they look the same as those untagged
        return "-----"
    return tag

def getGroups(instance):
    if len(instance.groups) == 1:
        # if there's only one group, then unpack it
        return instance.groups[0].name

    else:  # in the not-expected case where there is more than one groups, deal with it
        groupList = []
        for g in instance.groups:
            groupList.append(g.name)
        return groupList

def getInstanceOf(volume):
    # ughhhhhhhh refactor later
    creds = credentials()
    conn = ec2.connect_to_region(volume.region.name, **creds)
    ins_id = volume.attach_data.instance_id
    reservation = conn.get_all_instances(instance_ids=ins_id)[0]
    return reservation.instances[0]

def generateInfoVolumes(regions):
#    # Print volumes to screen
#    print "\n+ VOLUMES +"
#    print "volume_ID\tstate\tsize\ttime_stamp_volume_created\tregion(zone)\tsnapshot ID"
#    for r in regions:
#        volumes = getVolumes(r)
#        for v in volumes:
#            print "%s\t%s\t%s GB\t%s\t%s\t%s" % (v.id, v.status, v.size, v.create_time, v.zone, v.snapshot_id)

    # Write to file
    print "\nWriting volumes info to output file %s" % volumes_data_output_file
    with open(volumes_data_output_file, 'w') as f1:
        f1.write("VOLUMES\n")
        f1.write("KEEP-tag\tinstance-friend\tKEEP-tag_of_instance\tvolume_ID\tstate\tsize\ttime_stamp_volume_created\tregion(zone)\tsnapshot_ID\n")
        for r in regions:
            volumes = getVolumes(r)
#            import pdb; pdb.set_trace()
            print "."  #give some feedback to the user
            for v in volumes:
                f1.write ("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
                  % (getKeepTag(v), getInstanceOf(v), getKeepTag(getInstanceOf(v)), v.id, v.status, v.size, v.create_time, v.zone, v.snapshot_id) )

def generateInfoSnapshots(regions):
    # Snapshots
    print "Writing snapshots info to output file %s" % snapshots_data_output_file
    snapshots = []
    for r in regions:
        snapshots += getSnapshots(r)
        print "."  #feedback for the user
#    # Print snapshots to screen
#    print "\n+ SNAPSHOTS +"
#    print "snapshot_id\tstatus\tregion(availability_zone)\tprogress\tstart_date_time_stamp\tvolume_id\tvolume_size\tKEEP-tag"
#    for s in snapshots:
#           print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"
#                % (s.id, s.status, s.region, s.progress, s.start_time, s.volume_id, s.volume_size, getKeepTag(s))

    with open(snapshots_data_output_file, 'w') as f2:
        f2.write("SNAPSHOTS\n")
        f2.write("snapshot_id\tstatus\tregion(availability_zone)\tprogress\tstart_date_time_stamp\tvolume_id\tvolume_size\tKEEP-tag\n")
        for s in snapshots:
            f2.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
                 % (s.id, s.status, s.region.name, s.progress, s.start_time, s.volume_id, s.volume_size, getKeepTag(s)) )

def generateInfoInstances(regions):
    # Print instances to screen
#    print "\n+ INSTANCES +"
#    print "instance ID\tinstance_type\tstate\ttime_stamp_instance_launched\tsecurity_groups\tKEEP-tag"
#    for region in regions:
#        instances = getInstances(region)
#        for i in instances:
#            print "%s\t%s\t%s\t%s\t%s\t%s" % (i.id, i.instance_type, i.state, i.launch_time, getGroups(i), getKeepTag(i))

    print "Writing instances info to output file %s" % instances_data_output_file
    with open(instances_data_output_file, 'w') as f3:
        f3.write("INSTANCES\n")
        f3.write("instance ID\tinstance_type\tstate\ttime_stamp_instance_launched\tsecurity_groups\tKEEP-tag\n")
        for region in regions:
            print "."  #feedback for user
            instances = getInstances(region)
            for i in instances:
                f3.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (i.id, i.instance_type, i.state, i.launch_time, getGroups(i), getKeepTag(i)))

def generateInfoImages(regions):
    print "Writing images info to output file %s" % images_data_output_file
    with open(images_data_output_file, 'w') as f4:
        f4.write("IMAGES\n")
        f4.write("image_name\timage_id\tregion\tstate\tcreation_date\ttype\tKEEP-tag\tdescription\n")
        for r in regions:
            print "."  #feedback for user
            images = getImages(r)
            for im in images:
                f4.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
                        % (im.name, im.id, im.region.name, im.state, im.creationDate, im.type, getKeepTag(im), im.description) )



#TODO: add time-passed since launch (in lieu of time tagless)
#TODO: have AMIs-snaps, ins-vols mapped such that tagging lhs propagates to rhs
#TODO: have this stuff accessible from s3, public url

def main ():
    regions = getRegions()
#    import pdb; pdb.set_trace()
    generateInfoVolumes(regions)
#    generateInfoSnapshots(regions)
#    generateInfoInstances(regions)
#    generateInfoImages(regions)


if __name__ == '__main__':
    main()
