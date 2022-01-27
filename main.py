from batchers import create_batches, print_batch, update_batch_info, gen_report
from products import ProdFreqDB
import random
import os


dbfile = 'db_large.csv'
report_file = 'results/analysis.csv'
num_days = 30
pfd = ProdFreqDB(dbfile)

#for small db
DAILY_BATCHES = 6
MAX_BATCH_SIZE = 2

#for large db
DAILY_BATCHES = 40
MAX_BATCH_SIZE = 10

#remember to delete all files in results dir before starting

# Create batches for the next num_days days
for i in range(num_days):
	b = create_batches(DAILY_BATCHES, MAX_BATCH_SIZE, pfd)
	# Will cause problems on windows
	batch_record_file = 'results/batches_day{}.txt'.format(str(i+1))
	with open(batch_record_file,'w') as outfile:
		for b_num,batch in enumerate(b):
			outfile.write(print_batch(b_num+1, batch))


runs = {prod.name: 0 for prod in pfd.all_products}

with os.scandir('results') as results_dir:
	for result in results_dir:
		if result.is_file() and 'batches_day' in result.path:
			update_batch_info(result.path, runs)

# Create csv
gen_report(report_file, pfd, runs, num_days)