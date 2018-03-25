"""
Neutronstar
Create Database

"""
import sqlite3
import pandas as pd


"""
NOT NECESSARY WITH PANDAS AND SQLITE
Creating a context manager to handle the opening of Database. This does not save the edits in DB
"""


class dbopen(object):
    def __init__(self, path):
        self.path = path
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.path)
        self.cursor = self.conn.cursor()
        return self.conn

    def __exit__(self, exc_class, exc, traceback):
        # self.conn.commit()
        self.conn.close()

        if traceback:
            print("Exception has been handled: {}".format(str(type)))
            return True


"""
Class that creates a DataFrame from a csv (default to grab 1st col and row as index and header) - 
can be changed expanded to create some sort of data validation
"""


class AbstractCSVtoDF:
    def __init__(self, csv_filepath):
        self.csv_filepath = csv_filepath

    def validation(self):
        # raise error as method not defined in superclass
        raise NotImplementedError("validation method has not been implemented.")

    def load(self):
        newdf = pd.read_csv(self.csv_filepath, header=0, index_col=0)
        return newdf


if __name__ == '__main__':

    """
    pass filepath to DF creator class, then create DB connection and pass DF to sqlite DB
    """
    hpifilepath = "/Users/adrianotaylorescribano/PycharmProjects/HelloWorld/FMAC-HPI.csv"

    # create a Data Frame from csv
    hpiDF = AbstractCSVtoDF(hpifilepath).load()

    # simplistic way of creating SQLite DB using DF - could use context manager later on if needed
    conn = sqlite3.connect('USHPI.db')
    hpiDF.to_sql('USHPI.db', conn, flavor="sqlite",if_exists="fail",index=True)

