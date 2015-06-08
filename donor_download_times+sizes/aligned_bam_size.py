#!/usr/bin/env python
""" Collect data for aligned bam file sizes """

import urllib
import gzip
import json
import os

url = "http://pancancer.info/gnos_metadata/latest/donor_p_150608020205.jsonl.gz"

def get_raw_jsonl(url):
    """(Download and) unpack the jsonl file from pancancer.info"""
    if not os.path.isfile("donor_p_150608020205.jsonl.gz"):
        urllib.urlretrieve(url, "donor_p_150608020205.jsonl.gz")
    # unzip
    unzipped_jsonl = gzip.open("donor_p_150608020205.jsonl.gz", "rb")
    return unzipped_jsonl


def process_one_sample(json_obj):
    #stub
    print json_obj['aligned_tumor_specimens'][0]['aliquot_id']
############### this breaks if there is aligned_tumor_specimens is [], i.e. unaligned! ######

def main():
    donor_samples_jsonl = get_raw_jsonl(url)

    data = donor_samples_jsonl.read()
    datalines = data.splitlines()


    for i in xrange(len(datalines)):
        sample = json.loads(datalines[i])  # one line = one json object = one sample
        process_one_sample(sample)



    #select only ICGC
    #select only aligned

    #select control
    #sum tumour
    # add columns for # of tumours, unique donor_id, project id, repo, control/tumour



if __name__ == '__main__':
    main()