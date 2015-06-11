"""Collect data for Sanger run times"""

import json
jsonl_file = "ICGC_sanger_true.jsonl"


def get_wall_s(j):
    vcr = j.get('variant_calling_results')
    svc = vcr.get('sanger_variant_calling')
    wd = svc.get('workflow_details')
    vtm = wd.get('variant_timing_metrics')
    workflow = vtm.get('workflow')
    wall_s = workflow.get('Wall_s')
    return wall_s


def get_gnos_repo(j):
    gnos_repo = j.get('variant_calling_results').get('sanger_variant_calling').get('gnos_repo')
    if len(gnos_repo) == 1:
        return gnos_repo[0]
    elif len(gnos_repo) == 0:
        return ""
    else:
        return str(gnos_repo)


def main():

    print '\t'.join(["donor_unique_id", "Wall_s", "gnos_repo", "dcc_project_code"])
    total_time = 0
    number_donors = 0

    with open(jsonl_file) as f:
        for line in f:
            j = json.loads(line)
            wall_s = get_wall_s(j)
            # import pdb; pdb.set_trace()
            print '\t'.join([j.get('donor_unique_id'), str(wall_s), get_gnos_repo(j), j.get('dcc_project_code')])
            total_time += wall_s
            number_donors += 1

    print "\nTotal time:\t%d" % total_time
    print "Number of donors counted:\t%d" % number_donors

if __name__ == '__main__':
    main()
