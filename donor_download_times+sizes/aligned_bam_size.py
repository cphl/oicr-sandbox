#!/usr/bin/env python
""" Collect data for aligned bam file sizes """

import urllib
import gzip
import json
import os
import fnmatch

url = "http://pancancer.info/gnos_metadata/latest/donor_p_150608020205.jsonl.gz"

def get_raw_jsonl(url):
    """(Download and) unpack the jsonl file from pancancer.info"""
    if not os.path.isfile("donor_p_150608020205.jsonl.gz"):
        urllib.urlretrieve(url, "donor_p_150608020205.jsonl.gz")
    # unzip
    unzipped_jsonl = gzip.open("donor_p_150608020205.jsonl.gz", "rb")
    return unzipped_jsonl

def is_tcga(donor):
    """Check if this donor is from TCGA (US projects)"""
    return fnmatch.fnmatch(donor['dcc_project_code'], '*-US')


def process_one(donor_id, aligned_bam, type):
    """Get all information of interest from a specimen"""
    # control/tumour, unique donor id, gnos repo, bam file size, project-ID, number of tumour... # TODO other info?
    #stub
    print '\t'.join([donor_id, str(aligned_bam.get('bam_file_size')), type])
    return True


def main():
    donor_samples_jsonl = get_raw_jsonl(url)

    data = donor_samples_jsonl.read()
    list_of_jsons = data.splitlines()

    print "Number of donors before removing TCGA and unaligned: %d" % len(list_of_jsons)

    number_of_tcga_donors = 0
    number_of_aligned_controls = 0
    number_of_donors_with_aligned_tumour = 0
    number_of_tumour_specimens = 0

    for i in xrange(len(list_of_jsons)):
        donor = json.loads(list_of_jsons[i])  # one line = one json object = one donor
#        import pdb; pdb.set_trace()
        if is_tcga(donor):
            number_of_tcga_donors += 1
            continue  # disregard TCGA donors
        else:
            if donor.get('normal_alignment_status') and donor.get('normal_alignment_status').get('aligned_bam'):
                process_one(donor['donor_unique_id'], donor['normal_alignment_status']['aligned_bam'], 'control')
                number_of_aligned_controls += 1

            if donor.get('tumor_alignment_status'):
                for aligned_bam in donor.get('tumor_alignment_status'):
                    if aligned_bam.get('aligned_bam'):
                        process_one(donor['donor_unique_id'], aligned_bam.get('aligned_bam'), 'tumour')
                        number_of_tumour_specimens += 1

    print "Number of aligned controls: %d" % number_of_aligned_controls
    print "Number of tumour specimens: %d" % number_of_tumour_specimens

if __name__ == '__main__':
    main()