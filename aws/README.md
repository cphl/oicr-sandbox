# AWS tracking

This script generates 4 reports for: volumes, snapshots, instances, and AMIs on AWS.
All show the objects of interest with KEEP-tags and production tags.

Eventually these reports will be used to decide which resources are unused and can be deleted,
and how resources in-use are distributed over people.
Current convention is to have the value of the tag with key='KEEP' be the user's name in all capital letters.

Any resource with a KEEP-tag will be kept.
Any volume associated with an instance with a KEEP-tag,
and any snapshot associated with an AMI with a KEEP-tag will also be kept.


## Generating reports

Run this at terminal

        python usage.py

Four tab-delimited text files will be written to the current directory:
volumes.tsv, snapshots.tsv, instances.tsv, images.tsv


#### Requirements

1. AWS credentials. Have them set as environment variables
        AWS_ACCESS_KEY and AWS_SECRET_KEY

2. Known to work with Python 2.7.6 and requirements.txt


## Viewing the reports

The reports are easier to view and sort using a spreadsheet application.
Open or import the files selecting tabs for delimiters; do not use comma delimiters.


## Notes about the reports

* The information will be up to date from the time the script is run as it pulls the data directly from AWS.

* A blank KEEP-tag is possible, but it's intended to have a person's name. On the AWS web console, a KEEP-tag with an empty string for its value looks the same as something that has not been tagged at all.

* For the volumes report, some of the "associated snapshots" may no longer exist but are provided by AWS to show how they were related before.
