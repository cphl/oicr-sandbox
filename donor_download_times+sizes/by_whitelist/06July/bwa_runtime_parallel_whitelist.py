"""Calculate BWA runtimes, select only donors on given whitelist, allow parallelization."""
__author__ = 'cleung'

import json
import datetime
import math
import string
import csv
import pdb

output_file = "bwa_runtime_parallel.csv"
jsonl_file = "donor_p_150706092742.jsonl"
whitelist_file = "bwa.aws.whitelist.tsv"
number_of_processors = 2

class Donors(object):
    """This isn't a great name. It's the class for everything I'm doing here."""
    def __init__(self):
        self.whitelist = self.process_whitelist()
        self.whitelist_donors = self.select_whitelist_donors()
        self.tasks = ['bwa', 'merge', 'qc']
        self.data = []
        self.collect_data()
        # pdb.set_trace()

    @staticmethod
    def process_whitelist():
        """Grab the whitelist and change it to a usable form."""
        with open(whitelist_file) as f:
            data = f.read()
            temp = string.replace(data, "\t", "::")
            return temp.splitlines()

    def select_whitelist_donors(self):
        """Return list of dicts that correspond to only the selected donors."""
        donors = []
        with open(jsonl_file) as f:
            for line in f:
                j = json.loads(line)
                if j.get('donor_unique_id') in self.whitelist:
                    donors.append(j)
        return donors

    @staticmethod
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

        # Sum every (number_of_processor+1)-th and the first
        parallel_time_best = 0
        for i, time in enumerate(times):
            if int(math.fmod(i, number_of_processors)) == 0:
                parallel_time_best += time

        return dict(pt_worst=parallel_time_worst, pt_best=parallel_time_best)

    @staticmethod
    def get_best_and_worst_cases_full_time(ptimes):
        """Use dict of separate tasks' parallel times to get full runtimes"""
        best = sum([x['pt_best'] for x in ptimes.values()])
        worst = sum([x['pt_worst'] for x in ptimes.values()])
        return dict(best=best, worst=worst)

    def datafy_one_specimen(self, spec, spec_type, donor_unique_id):
        """Fill in the desired data for one specimen entry"""

        ptimes = {}
        for task in self.tasks:
            ptimes[task] = self.get_parallel_times(spec, task, number_of_processors)

        full_time = self.get_best_and_worst_cases_full_time(ptimes)
        # ok well... i dunno... i wish i knew how to parametrize this nicely
        self.data.append({'donor_unique_id': donor_unique_id,
                          'specimen type': spec_type,
                          'bwa_parallel_best': ptimes['bwa']['pt_best'],
                          'bwa_parallel_worst': ptimes['bwa']['pt_worst'],
                          'merge_parallel_best': ptimes['merge']['pt_best'],
                          'merge_parallel_worst': ptimes['merge']['pt_worst'],
                          'qc_parallel_best': ptimes['qc']['pt_best'],
                          'qc_parallel_worst': ptimes['qc']['pt_worst'],
                          'full_time_best': full_time['best'],
                          'full_time_worst': full_time['worst']
                          })


    def collect_data(self):
        """Grab fields from json, keep separate in case we want more details in future"""
        print "Collecting data..."
        for d in self.whitelist_donors:
            # process the control
            if 'normal_specimen' in d:
                spec = d.get('normal_specimen')
                if spec.get('is_aligned'):
                    self.datafy_one_specimen(spec, "control", d['donor_unique_id'])

            # process the tumour(s), if any

    def generate_report(self):
        """Output the info to a file"""

        # For now, just dump it, to take a look

        print "Data being written to file " + output_file + "..."
        with open(output_file, 'w') as f:
            fields = ['donor_unique_id', 'specimen type', 'bwa_parallel_best', 'bwa_parallel_worst',
                      'merge_parallel_best', 'merge_parallel_worst', 'qc_parallel_best', 'qc_parallel_worst',
                      'full_time_best', 'full_time_worst']
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for donor in self.data:
                writer.writerow({'donor_unique_id': donor['donor_unique_id'],
                                 'specimen type': donor['specimen type'],
                                 'bwa_parallel_best': donor['bwa_parallel_best'],
                                 'bwa_parallel_worst': donor['bwa_parallel_worst'],
                                 'merge_parallel_best': donor['merge_parallel_best'],
                                 'merge_parallel_worst': donor['merge_parallel_worst'],
                                 'qc_parallel_best': donor['qc_parallel_best'],
                                 'qc_parallel_worst': donor['qc_parallel_worst'],
                                 'full_time_best': donor['full_time_best'],
                                 'full_time_worst': donor['full_time_worst']})


def main():
    D.generate_report()


if __name__ == '__main__':
    D = Donors()
    main()
