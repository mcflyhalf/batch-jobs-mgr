from math import ceil
import csv


class ProdFreqDB:
    """Wrapper that provides convenient access and functionality to a product-frequency database

    :param dbfile: path to a csv db file that contains product-frequency information
    """

    def __init__(self, dbfile):
        self.dbfile = dbfile
        self._db_dict = self._read_dbfile(self.dbfile)

    def refresh(self):
        """Refresh required if db file changes"""
        self.__init__(self.dbfile)

    def _read_dbfile(self, dbfile):
        with open(dbfile, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            _db_dict = {}
            for prod in reader:
                _db_dict[prod["Product"]] = float(prod["Frequency"])
            return _db_dict

    @property
    def required_daily_scrapes(self):
        """
        Comb through db and determine total number of scrapes required.
        Total number of scrapes is simply the sum of all daily frequencies.
        """
        # This fxn can be cached
        total_scrapes = 0
        prods = self.all_products

        for prod in prods:
            total_scrapes += prod.frequency

        return ceil(total_scrapes)

    @property
    def all_products(self):
        """
        Return a list of all products in the db

        :return: List of products and their frequencies in db
        """
        products = list()
        for name, freq in self._db_dict.items():
            products.append(Product(name=name, frequency=freq))
        return products


class Product:
    """
    ORM representation of a product in the db

    :param name: Product's name
    :param frequency: Frequency of scraping daily e.g 24 means 24 times a day and 0.25 means once every 4 days
    """

    def __init__(self, name: str, frequency: float):
        self.name = name
        self.frequency = frequency
