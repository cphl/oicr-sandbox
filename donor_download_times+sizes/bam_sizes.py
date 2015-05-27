# Niall's magic
import json
import urllib2

filename = "ICGC_only_donor_p_150520020206.jsonl"

with open(filename) as f:
        data = f.readlines()

donors = []

for line in data:
	jason = json.loads(line)
	donors.append(jason["donor_unique_id"])

# Process the damn jasons
for donor in donors:
	response = urllib2.urlopen("http://pancancer.info/elasticsearch/pcawg_es/donor/%s" % (donor))
        string = response.read()
	response.close()
	j = json.loads(string)	
	data = { 'tumour': "", 'control': "" }
	for d in data:
		data[d] = { 'size': 0, 'repo': "?" }
	bamfiles = j['_source']['bam_files']
	for bam in bamfiles:
		if bam['is_aligned'] is True:
			continue
	        data_key = ""	
		if bam['use_cntl'] == "N/A":
			data_key = "control"
		else:
			data_key = "tumour"
		data[data_key]['size'] += bam['bam_file_size']
		data[data_key]['repo'] = bam['gnos_repo']
	print "%s,%s,%s,%s" % (donor, 'control', data['control']['size'], data['control']['repo'])
	print "%s,%s,%s,%s" % (donor, 'tumour', data['tumour']['size'], data['tumour']['repo'])
