# example from http://boto.readthedocs.org/en/latest/getting_started.html
import boto
import time

# instantiate new client
s3 = boto.connect_s3()

# make new bucket with unique name : this part is free
b = s3.create_bucket('boto-example-heyyyyyyyyyyyyyyy-%s' % int(time.time()))

# make new k/v pair: storing and transferring stuff out of bucket DOES cost money $$
k = b.new_key('my_key')
k.set_contents_from_string("These are the guts. The valuable guts.")

# Give the data time to travel
time.sleep(2)

# Retrieve contents of the key above
print k.get_contents_as_string()

print "You got 6 seconds to see it show up on the web console."

time.sleep(6)

# delete key (actually the object associated with the key??)
k.delete()

# delete the bucket
b.delete()
