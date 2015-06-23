"""Collect data for BWA runtimes using whitelist to grab particular donors only"""

import json
import datetime
import math

jsonl_file = "donor_p_150622020204.jsonl"
whitelist_file = "whitelist.txt"    # preformat with "::" in place of " " (space) to make donor_unique_id
                                    # TODO make this better: format or handle it here
number_of_processors = 2

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


def get_parallel_times(specimen, task, number_of_processors):
    """Return amount of time it takes for a specimen to finish a task ('bwa', 'merge', or 'qc'),
    given that number_of_processors can run in parallel.
    """
    times = []
    task_timing = task + "_timing_seconds"
    lanes_timings = specimen.get('alignment').get('timing_metrics')
    for lane in lanes_timings:  # grab all the times for all lanes in this specimen
        if 'metrics' in lane:
            times.append(lane.get('metrics').get(task_timing))
    times = sorted([int(t) for t in times], reverse=True)
    number_of_lanes = len(lanes_timings)
    number_of_runs = int(math.ceil(number_of_lanes/float(number_of_processors)))
    parallel_time_worst = sum(times[0:number_of_runs])
    # Sum every (number_of_processor+1)-th and the first.
    parallel_time_best = 0
    for i in range(len(times)):
        if int(math.fmod(i, number_of_processors)) == 0:
            parallel_time_best += times[i]
    return parallel_time_best, parallel_time_worst


def get_best_worst_cases_full_time(timings_list):
    """Take a list of timings, paired by task, best then worst
    (i.e. [bwa best case, bwa worst case, merge best case, merge worst case, qc best case, qc worst case])
    Return full time for best case and full time for worst case.
    """
    full_best = sum([timings_list[i] for i in range(len(timings_list)) if math.fmod(i, 2) == 0])
    full_worst = sum([timings_list[i] for i in range(len(timings_list)) if math.fmod(i, 2) == 1])
    return full_best, full_worst


def main():

    whitelist_donors = select_whitelist_donors()

    # with open('whitelist_donors.jsonl', 'w') as f:
    #     for wd in whitelist_donors:
    #         json.dump(wd, f)
    #         f.flush()

    print '\t'.join(["donor_unique_id", "type", "bwa_parallel_best", "bwa_parallel_worst", "merge_parallel_best",
                     "merge_parallel_worst", "qc_parallel_best", "qc_parallel_worst", "full_parallel_best",
                     "full_parallel_worst"])

    tasks = ['bwa', 'merge', 'qc']
    controls_counted = 0
    specimens_counted = 0
    total_best_time = 0
    total_worse_time = 0
    parallel_timings = []

    for d in whitelist_donors:

        # process control
        if 'normal_specimen' in d:
            specimen_type = "control"
            spec = d.get('normal_specimen')
            if spec.get('is_aligned'):
                for task in tasks:
                    parallel_timings.extend(get_parallel_times(spec, task, number_of_processors))

                (full_best, full_worst) = get_best_worst_cases_full_time(parallel_timings)
                dataline = [d['donor_unique_id'], specimen_type] + parallel_timings + [full_best, full_worst]
                print('\t'.join(map(str, dataline)))

                controls_counted += 1
                specimens_counted += 1
                total_best_time += full_best
                total_worse_time += full_worst
            parallel_timings = []

        # process tumour
        if 'aligned_tumor_specimens' in d:
            specimen_type = "tumour"
            specs = d.get('aligned_tumor_specimens')
            for spec in specs:
                if spec.get('is_aligned'):
                    for task in tasks:
                        parallel_timings.extend(get_parallel_times(spec, task, number_of_processors))

                    (full_best, full_worst) = get_best_worst_cases_full_time(parallel_timings)
                    dataline = [d['donor_unique_id'], specimen_type] + parallel_timings + [full_best, full_worst]
                    print('\t'.join(map(str, dataline)))

                    specimens_counted += 1
                    total_best_time += full_best
                    total_worse_time += full_worst
            parallel_timings = []

    print "\nControls counted:\t%d" % controls_counted
    print "Specimens counted:\t%d" % specimens_counted
    print "\nBased on %dx-parallelization..." % number_of_processors
    print "Total best case time (optimal parallelization):\t%d" % total_best_time
    print "Total worst case time (least optimal parallelization for each specimen):\t%d" % total_worse_time
    print "Estimated best time per donor (by # of controls):\t%d\tor in days:\t%.2f" %\
          (total_best_time/controls_counted, total_best_time/controls_counted/3600./24.)
    print "Estimated worst time per donor (by # of controls):\t%d\tor in days:\t%.1f" %\
          (total_worse_time/controls_counted, total_worse_time/controls_counted/3600./24.)
    print "Estimated best time per specimen:\t%d\tor in days:\t%.2f" %\
          (total_best_time/specimens_counted, total_best_time/specimens_counted/3600./24.)
    print "Estimated worst time per specimen:\t%d\tor in days:\t%.2f" %\
          (total_worse_time/specimens_counted, total_worse_time/specimens_counted/3600./24.)


if __name__ == '__main__':
    main()
