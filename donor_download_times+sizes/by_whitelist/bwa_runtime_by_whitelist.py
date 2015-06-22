"""Collect data for BWA runtimes:
- using whitelist to grab particular donors only,
- using within a particular time frame.
"""

import json
import datetime

jsonl_file = "donor_p_150622020204.jsonl"
whitelist_file = "whitelist.txt"    # preformat with "::" in place of " " (space) to make donor_unique_id
                                    # TODO make this better: format or handle it here
start_date = 2015-06-16

def donor_on_whitelist(donor_dict):
    """True if this donor (represented by this json-dict) is on the whitelist."""
    # ugh is there a better way? Seems to work
    whitelist = []
    with open(whitelist_file) as f:
        data = f.read()
        whitelist = data.splitlines()
    for d in whitelist:
        # check if this is the key
        if donor_dict.get('donor_unique_id') == d:
            return True
    return False


def select_whitelist_donors():
    """Return list of dicts that correspond to only the selected donors.
    donor_list -- text file with one donor_id on each line
    """
    donors = []
    with open(jsonl_file) as f:
        for line in f:
            j = json.loads(line)
            if donor_on_whitelist(j):
                donors.append(j)
    return donors

def get_times(lanes_timings):
    """ Input is a list of lanes' timings.

        Grab 4 values from json:    bwa_timing_seconds,
                                    download_timing_seconds,
                                    merge_timing_seconds,
                                    qc_timing_seconds
        Return dict of...lots of stuff
    """

    # Assume all lanes run parallel for a particular process. This gives an absolute lower bound (specimen level).
    bwa_max = 0
    download_max = 0
    merge_max = 0
    qc_max = 0
    summaxes = 0

    # If all lanes run sequentially for each step, this gives the upper bound. (Absolute wrt these metrics)
    bwa_sum = 0
    download_sum = 0
    merge_sum = 0
    qc_sum = 0
    sumsums = 0

    no_dl = 0

    for lane in lanes_timings:
        if 'metrics' in lane:
            metrics = lane.get('metrics')
            bwa = metrics.get('bwa_timing_seconds')
            download = metrics.get('download_timing_seconds')
            merge = metrics.get('merge_timing_seconds')
            qc = metrics.get('qc_timing_seconds')

            bwa_max = max(bwa_max, bwa)
            download_max = max(download_max, download)
            merge_max = max(merge_max, merge)
            qc_max = max(qc_max, qc)

            bwa_sum += bwa
            download_sum += download
            merge_sum += merge
            qc_sum += qc

            sumsums += bwa + merge + qc + download
            no_dl += bwa + merge + qc
    summaxes += bwa_max + merge_max + qc_max  + download_max

    return {'bwa_max': bwa_max, 'bwa_sum': bwa_sum, 'download_max': download_max, 'download_sum': download_sum,
            'merge_max': merge_max, 'merge_sum': merge_sum, 'qc_max': qc_max, 'qc_sum': qc_sum,
            'sum_of_max_times': summaxes, 'sum_all': sumsums, 'no_dl': no_dl}



def get_gnos_repo(specimen_key):
    """This way does not get you the place the workflow was processed. Need to cross reference with whitelists.
        But maybe it gets us the repo it was downloaded from?
    """
    return specimen_key.get('gnos_repo')


def main():

    whitelist_donors = select_whitelist_donors()

    # with open('whitelist_donors.jsonl', 'w') as f:
    #     for wd in whitelist_donors:
    #         json.dump(wd, f)
    #         f.flush()

    print '\t'.join(["donor_unique_id", "type", "bwa_max", "bwa_sum", "download_max", "download_sum", "merge_max",
                     "merge_sum", "qc_max", "qc_sum", "sum_of_max_times", "sum_all", "no_dl"])
    total_time = 0
    total_parallel_time = 0
    number_donors = 0

    for d in whitelist_donors:

        # Process the control (there should always be exactly 1)
        if 'normal_specimen' in d:
            specimen_type = "control"
            control_specimen = d.get('normal_specimen')
            if control_specimen.get('is_aligned'):
                lanes_timings = control_specimen.get('alignment').get('timing_metrics')
                times = get_times(lanes_timings)

            print '\t'.join([d.get('donor_unique_id'), specimen_type, str(times['bwa_max']), str(times['bwa_sum']),
                             str(times['download_max']), str(times['download_sum']), str(times['merge_max']),
                             str(times['merge_sum']), str(times['qc_max']), str(times['qc_sum']),
                             str(times['sum_of_max_times']), str(times['sum_all']), str(times['no_dl'])])
            total_time += times['sum_all']
            total_parallel_time += times['sum_of_max_times']

        # Process the tumor specimen(s) if they exist
        if 'aligned_tumor_specimens' in d:
            specimen_type = "tumour"
            tumor_specimens = d.get('aligned_tumor_specimens')
            # possibly more than 1 lane for tumor, so need to iterate through

            for t in tumor_specimens:
                if t.get('is_aligned'):
                    lanes_timings = t.get('alignment').get('timing_metrics')
                    times = get_times(lanes_timings)
                # import pdb; pdb.set_trace()
                print '\t'.join([d.get('donor_unique_id'), specimen_type, str(times['bwa_max']), str(times['bwa_sum']),
                                 str(times['download_max']), str(times['download_sum']), str(times['merge_max']),
                                 str(times['merge_sum']), str(times['qc_max']), str(times['qc_sum']),
                                 str(times['sum_of_max_times']), str(times['sum_all']), str(times['no_dl'])])
                total_time += times['sum_all']
                total_parallel_time += times['sum_of_max_times']

        number_donors += 1

    print "\nTotal time:\t%d" % total_time
    print "Total time with full parallelization within a specimen: \t%d" % total_parallel_time
    print "Total donors included:\t%d" % number_donors
    time_per_donor_full = total_time / number_donors
    time_per_donor_parallel = total_parallel_time / number_donors
    print "Time per donor (full):\t%d\tor in days:\t%.1f" % (time_per_donor_full, (time_per_donor_full/3600./24.))
    print "Time per donor (full parallelization):\t%d\tor in days:\t%.1f" % (time_per_donor_parallel, (time_per_donor_parallel/3600./24.))

if __name__ == '__main__':
    main()
