from math import ceil
import csv

class ProdFreqDB:
	"""ProdFreqDB is a wrapper that provides convenient access and functionality to a product-frequency database"""
	def __init__(self, dbfile):
		self.dbfile = dbfile
		self._db_dict = self._read_dbfile(self.dbfile)

	def refresh(self):
		'''Refresh required if db file changes'''
		self.__init__(self.dbfile)

	def _read_dbfile(self, dbfile):
		with open(dbfile,'r') as csvfile:
			reader = csv.DictReader(csvfile)
			_db_dict = {}
			for prod in reader:
				_db_dict[prod['Product']] = float(prod['Frequency'])
			return _db_dict

	@property
	def required_daily_scrapes(self):
		'''Comb through db and determine total number of scrapes required'''
		# This fxn can be cached
		total_scrapes = 0
		prods = self.all_products

		for prod in prods:
			total_scrapes += prod.frequency

		return ceil(total_scrapes)
	
	@property
	def all_products(self):
		products = list()
		for name, freq in self._db_dict.items():
			products.append(Product(name=name, frequency=freq))
		return products


class Product:
	def __init__(self, name: str, frequency: float):
		self.name = name
		self.frequency = frequency