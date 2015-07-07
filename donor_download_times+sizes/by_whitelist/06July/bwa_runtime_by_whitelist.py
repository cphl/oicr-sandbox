"""Collect data for BWA runtimes using whitelist to grab particular donors only"""
### Do this for the reports Brian wants for AWS
### Get the white lists by: cat `find . | grep txt | grep aws | grep -v black | grep bwa_alignement` > bwa.aws.whitelist.tsv 
#TODO: something is weird when there is more than one tumour for a donor, check out the results

import json
import datetime
import math

jsonl_file = "donor_p_150706092742.jsonl"
whitelist_file = "bwa.aws.whitelist.tsv.regex"  # make sure has "::" in place of " " (space) to make donor_unique_id
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


def get_download_data(donor):
    """Returns: - GNOS repo from which the source data for BWA was taken (lane)
                - download time (lane)
    """
    repo = ""
    download_timing = 0
    return repo, download_timing


def generate_download_report(donors):
    """Gather data on download times and from which repo for these donors and generate some stats"""
    #stub
    pass


def main():

    whitelist_donors = select_whitelist_donors()

    generate_download_report(whitelist_donors)

    print '\t'.join(["donor_unique_id", "type", "bwa_parallel_best", "bwa_parallel_worst", "merge_parallel_best",
                     "merge_parallel_worst", "qc_parallel_best", "qc_parallel_worst", "full_parallel_best",
                     "full_parallel_worst"])

    tasks = ['bwa', 'merge', 'qc']
    controls_counted = 0
    specimens_counted = 0
    total_best_time = 0
    total_worst_time = 0
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
                total_worst_time += full_worst
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
                    total_worst_time += full_worst
            parallel_timings = []

    print "\nControls counted:\t%d" % controls_counted
    print "Specimens counted:\t%d" % specimens_counted
    print "\nBased on %dx-parallelization..." % number_of_processors
    print "Total best case time (optimal parallelization):\t%d" % total_best_time
    print "Total worst case time (least optimal parallelization for each specimen):\t%d" % total_worst_time
    print "Estimated best time per donor (by # of controls):\t%d\tor in days:\t%.2f" %\
          (total_best_time/controls_counted, total_best_time/controls_counted/3600./24.)
    print "Estimated worst time per donor (by # of controls):\t%d\tor in days:\t%.1f" %\
          (total_worst_time/controls_counted, total_worst_time/controls_counted/3600./24.)
    print "Estimated best time per specimen:\t%d\tor in days:\t%.2f" %\
          (total_best_time/specimens_counted, total_best_time/specimens_counted/3600./24.)
    print "Estimated worst time per specimen:\t%d\tor in days:\t%.2f" %\
          (total_worst_time/specimens_counted, total_worst_time/specimens_counted/3600./24.)


if __name__ == '__main__':
    main()
