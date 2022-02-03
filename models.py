from sqlalchemy import Column, Integer, String, DateTime, Float, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
import csv
from math import ceil


Base = declarative_base()


class ProdFreqDB:
    """Wrapper that provides convenient access and functionality to a product-frequency database

    :param session: appropriate sqlalchemy session object
    """

    def __init__(self, session):
        self.session = session

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
        products = self.session.query(Product).all()
        return products


class Product(Base):
    """
    ORM representation of a product in the db

    :param name: Product's name
    :param frequency: Frequency of scraping daily e.g 24 means 24 times a day and 0.25 means once every 4 days
    """

    __tablename__ = "product"

    id = Column(Integer, primary_key=True)
    name = Column(String(20), unique=True, nullable=False)
    frequency = Column(Float, nullable=False)
    last_batched = Column(DateTime, nullable=False)

    def __repr__(self):
        return "Product(name= {}, frequency= {})".format(self.name, self.frequency)


class ProdConfig:
    def __init__(self, engine, prod_file):
        self.new_db_config(engine, prod_file)

    def create_tables(self, engine):
        tables = Base.metadata.sorted_tables
        Base.metadata.create_all(engine, tables=tables)

    # Delete all tables (DB should have been created in advance)
    def drop_tables(self, engine):
        tables = Base.metadata.sorted_tables
        tables.reverse()
        Base.metadata.drop_all(engine, tables=tables)

    def populate_db(self, prod_file, engine):
        Session = sessionmaker(bind=engine)
        with Session() as session, session.begin():
            with open(prod_file, "r") as csvfile:
                reader = csv.DictReader(csvfile)
                for prod in reader:
                    session.add(
                        Product(
                            name=prod["Product"],
                            frequency=prod["Frequency"],
                            last_batched=datetime.datetime.now() - datetime.timedelta(days=1),
                        )
                    )

    def new_db_config(self, engine, prod_file):  # For a new install
        self.drop_tables(engine)
        self.create_tables(engine)
        self.populate_db(prod_file, engine)


def get_session():
    Sessionmaker = sessionmaker(bind=engine)
    return Sessionmaker()


engine = create_engine("sqlite:///batch_products.db")
