"""Get file sizes for Sanger workflow"""
import json

jsonl_file = "ICGC_sanger_true.jsonl"

def get_size(j):
    """ Return sum of all the little files that contribute to this donor's Sanger run."""
    somesize = 0
    file_list = j.get('variant_calling_results').get('sanger_variant_calling').get('files')
    for f in file_list:
        somesize += int(f.get('file_size'))
    return somesize


def main():

    print '\t'.join(["donor_unique_id", "size of files"])
    total_size = 0
    number_donors = 0

    with open(jsonl_file) as f:
        for line in f:
            j = json.loads(line)
            # import pdb; pdb.set_trace()
            donor_size = get_size(j)
            print '\t'.join([j.get('donor_unique_id'), str(donor_size)])
            total_size += donor_size
            number_donors += 1

    print "\nTotal file size:\t%d" % total_size
    print "Total donors included:\t%d" % number_donors

if __name__ == '__main__':
    main()