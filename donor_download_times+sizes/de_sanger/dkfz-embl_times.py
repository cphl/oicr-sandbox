"""Collect data for DKFZ/EMBL run times"""

import json
jsonl_file = "ICGC_both_DKFZ_and_EMBL.jsonl"


def get_sum_times(j):
    """Sum over 5 values:   dkfz_reference_seconds,
                            dkfz_timing_seconds,
                            download_timing_seconds,
                            embl_timing_seconds,
                            reference_timing_seconds
    """
    vcr = j.get('variant_calling_results')
    dvc = vcr.get('dkfz_variant_calling')
    wd = dvc.get('workflow_details')
    vtm = wd.get('variant_timing_metrics')
    tm = vtm.get('timing_metrics')
    # Why is tm a list? Oh well, deal with it
    sometimes = 0
    for dictionnaire in tm:
        workflow = dictionnaire.get('workflow')
        sometimes += workflow.get('download_timing_seconds')
        sometimes += workflow.get('dkfz_reference_seconds')
        sometimes += workflow.get('embl_timing_seconds')
        sometimes += workflow.get('reference_timing_seconds')
        sometimes += workflow.get('dkfz_timing_seconds')
    return sometimes


def get_dkfz_gnos_repo(j):
    gnos_repo = j.get('variant_calling_results').get('dkfz_variant_calling').get('gnos_repo')
    if len(gnos_repo) == 1:
        return gnos_repo[0]
    elif len(gnos_repo) == 0:
        return ""
    else:
        return str(gnos_repo)


def get_embl_gnos_repo(j):  # same thing, lazy, sorry
    gnos_repo = j.get('variant_calling_results').get('embl_variant_calling').get('gnos_repo')
    if len(gnos_repo) == 1:
        return gnos_repo[0]
    elif len(gnos_repo) == 0:
        return ""
    else:
        return str(gnos_repo)


def main():

    print '\t'.join(["donor_unique_id", "sum of dkfz, embl, download, refs", "DKFZ gnos_repo", "EMBL gnos_repo", "dcc_project_code"])
    total_time = 0
    number_donors = 0

    with open(jsonl_file) as f:
        for line in f:
            j = json.loads(line)
            sum_times = get_sum_times(j)
            # import pdb; pdb.set_trace()
            print '\t'.join([j.get('donor_unique_id'), str(sum_times), get_dkfz_gnos_repo(j), get_embl_gnos_repo(j), j.get('dcc_project_code')])
            total_time += sum_times
            number_donors += 1

    print "\nTotal time:\t%d" % total_time
    print "Total donors included:\t%d" % number_donors

if __name__ == '__main__':
    main()
