import os
import sqlite3
import pandas as pd

def quantQuoteData(folderPath, tableName, databasePaths):
    #Set SQLite3 strings:
    createTable = '''CREATE TABLE IF NOT EXISTS {}(Date INTEGER, Time INTEGER, Open REAL, High REAL, Low REAL,
                    Close REAL, Volume REAL, Splits INTEGER, Earnings REAL, Dividends REAL)'''.format(tableName)
    #Create tables in each database:
    for x in databasePaths:
        conn = sqlite3.connect(x)
        try:
            conn.execute(createTable)
            conn.commit()
        finally:
            conn.close()
    #Walk through folder and append to database.
    for root, dirs, files in os.walk(folderPath):
        for name in files:
            if '.csv' in name:
                df = pd.read_csv(os.path.join(root,name), header = None)
                df.columns=['Date', 'Time', 'Open', 'High', 'Low', 'Close',
                              'Volume', 'Splits', 'Earnings', 'Dividends']
            for x in databasePaths:
                conn = sqlite3.connect(x)
                try:
                    df.to_sql(tableName, conn, index=False, if_exists='append')
                    conn.commit()
                finally:
                    conn.close()

def pullFromDatabase(tableName, databasePath, date, times, columns):
    pass

class data:
    def __init__(self, ticker):
        self.ticker = ticker
        self.dataSets = []
    def addDataSet(self, df, period, granularity, columns):
        