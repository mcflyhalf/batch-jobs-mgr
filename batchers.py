from products import ProdFreqDB
from typing import List, Dict
from math import ceil
import random

def create_batches(num_batches: int, max_batch_size: int, db: ProdFreqDB) -> List[str]:
	'''
	Create a number of batches. Presumed this will be run once daily
	:param num_batches: Total number of batches to create
	:param max_batch_size: Maximum size of each batch. Size <= max_batch_size
	:param prod_freq_db: database listing of how frequently (daily) each product should be batched. 
	12 means it should exist in 12 batches per day and 0.25 means it should exist in a batch every 4 days 
	'''
	if num_batches <= 0:
		raise ValueError("Cannot have 0 or negative number of batches. Current number: {}".\
						format(num_batches))
	if max_batch_size < 1:
		raise ValueError("Cannot have 0 or negative maximum batch size. Current number: {}".\
						format(max_batch_size))
	available_product_scrapes = num_batches * max_batch_size
	required_product_scrapes = db.required_daily_scrapes
	batch_utilisation_factor = required_product_scrapes/available_product_scrapes

	if batch_utilisation_factor > 1:
		batch_utilisation_factor = 1

	batch_size = ceil(max_batch_size * batch_utilisation_factor)
	batches = list()

	for i in range(num_batches):
		batch = create_batch(batch_size, num_batches, db)
		batches.append(batch)

	return batches

def create_batch(batch_size: int, daily_num_batches: int,  db: ProdFreqDB) -> str:
	prods = db.all_products
	random.shuffle(prods)

	batch = list()
	for prod in prods:
		#Uniform random distribution ensures average frequency over long time is achieved
		# if random.uniform(0,1) < (prod.frequency / daily_num_batches):
		if random.random() < (prod.frequency / daily_num_batches):
			batch.append(prod.name)
		if len(batch) >= batch_size:
			#log this action
			break
	return batch

def print_batch(id: str, batch: List[str]):
	txt = 'Batch {}:\t'.format(str(id))
	for prod_name in batch:
		txt += "{},".format(prod_name)
	else:
		txt = txt[:-1] + '\n'

	return txt

def update_batch_info(filepath: str, runs: Dict[str, int]):
	with open(filepath,'r') as res_file:
		for row in res_file:
			prods = row.split(':')[-1]
			prods = prods.split(',')
			for prod in prods:
				prod = prod.strip()
				if not prod: #In case of empty batch
					continue
				if str(prod) in runs.keys():
					runs[str(prod)] += 1
				else:
					runs[str(prod)] = 1

def gen_report(report_filepath, pfd, run_cuminfo, num_days):
	with open(report_filepath, 'w') as report:
		header_template = '{}\t{}\t{}\t{}\n'
		row_template = '{}\t{}\t{}\t{:.2%}\n' #Same as header template but with format specifier
		header_template = header_template.replace('\t',',')
		row_template = row_template.replace('\t',',')
		# print("creating header row")
		report.write(header_template.\
			format('Name', 'Desired frequency', 'Actual Frequency', 'Actual/Desired rate'))
		for product in pfd.all_products:
			total_freq = product.frequency*num_days
			report.write(row_template.\
				format(product.name,total_freq,\
					   run_cuminfo[product.name],\
					   run_cuminfo[product.name]/total_freq))
