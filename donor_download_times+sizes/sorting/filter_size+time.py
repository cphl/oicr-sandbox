
file1="./file_sizes.csv"
file2="./download_times.csv"

with open(file1) as f:
	data1=f.readlines()

with open(file2) as f:
	data2=f.readlines()

for line1 in data1:
#	donor1=line1.split(',')[0]
#	for line2 in data2:
#		donor2=line2.split(',')[1]
#		if donor1 == donor2:
#			print line1 + ',' + line2
	for line2 in data2:
		if (line1.split(',')[0] == line2.split(',')[0]) and (line1.split(',')[1] == line2.split(',')[1]):
			print line1 + ',' + line2
