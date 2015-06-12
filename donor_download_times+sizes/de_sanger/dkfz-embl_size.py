"""Get file sizes for DKFZ/EMBL workflows"""
import json

jsonl_file = "ICGC_both_DKFZ_and_EMBL.jsonl"

def get_size(files):
    """ Return sum of all the little files that contribute to this donor's DKFZ or EMBL run."""
    somesize = 0
    for f in files:
        somesize += int(f.get('file_size'))
    return somesize


def main():

    print '\t'.join(["donor_unique_id", "size of dkfz files", "size of embl files"])
    total_size_dkfz = 0
    total_size_embl = 0
    number_donors = 0

    with open(jsonl_file) as f:
        for line in f:
            j = json.loads(line)
            # import pdb; pdb.set_trace()
            dkfz_file_list = j.get('variant_calling_results').get('dkfz_variant_calling').get('files')
            dkfz_size = get_size(dkfz_file_list)
            embl_file_list = j.get('variant_calling_results').get('embl_variant_calling').get('files')
            embl_size = get_size(embl_file_list)
            print '\t'.join([j.get('donor_unique_id'), str(dkfz_size), str(embl_size)])
            total_size_dkfz += dkfz_size
            total_size_embl += embl_size
            number_donors += 1

    print "\nTotal DKFZ file size:\t%d" % total_size_dkfz
    print "Total EMBL file size:\t%d" % total_size_embl
    print "Total donors included:\t%d" % number_donors

if __name__ == '__main__':
    main()