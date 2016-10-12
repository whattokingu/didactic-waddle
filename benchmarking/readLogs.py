import sys
import re
print "Running"
with open(sys.argv[1] + "csv", 'w') as w:
	with open(sys.argv[1], 'r') as f:
		for line in f:
			group = re.search(r'\d+ transactions', line)
			if group:
				xact = re.search(r'\d+', group.group(0)).group(0)
				timegroup = re.search(r'\d+\.', line)
				timing = re.search(r'\d+', timegroup.group(0)).group(0)
				w.write(xact + ',' + timing + '\n')