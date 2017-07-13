import os
import sqlite3
import pandas as pd


class TickerData:
    #Class for insertion and retrieval of all data related to a given ticker.
    pass

class Analyze:
    #Class containing functions to analyze a single set of data.
    #Would like to add functions for: Re-sampling, reversing, indicators.
    def __init__(self, df, yahoo=False):
        print('''WARNING: Analyze assumes data is formatted Oldest to Newest, and that \ 
                columns [Open, High, Low, Close, Volume] exist and are correct. \
                When using Yahoo data, set yahoo=True or run self.yahoo to change Close = Adj Close.''')
        self.df = df
        if yahoo==True:
            self.yahoo()
        self.df['highOpen'] = (self.df.High - self.df.Open) / self.df.Open
        self.df['lowOpen'] = (self.df.Low - self.df.Open) / self.df.Open
        self.df['lowOpenAbs'] = abs(self.df.lowOpen)
        self.df['closeOpen'] = (self.df.Close - self.df.Open) / self.df.Open
        self.df['openPrevClose'] = (self.df.Open.shift(-1) - self.df.Close) / self.df.Close
        self.df['highPrevClose'] = (self.df.High.shift(-1) - self.df.Close) / self.df.Close
        self.df['lowPrevClose'] = (self.df.Low.shift(-1) - self.df.Close) / self.df.Close
        self.df['lowPrevCloseAbs'] = abs(self.df.lowPrevClose)
        self.df['closePrevClose'] = (self.df.Close.shift(-1) - self.df.Close) / self.df.Close

        self.filterSet =[-.05, -.045, -.04, -.035, -.03, -.025, -.02, -.0175, -.015, -.0125, -.01,
                     -.009, -.008, -.007, -.006, -.005, -.004, -.003, -.002, -.001, 0, .001,
                     .002, .003, .004, .005, .006, .007, .008, .009, .01, .0125, .015, .0175, .02,
                     .025, .03, .035, .04, .045, .05]
        self.dfSimpleProbabilities= None

    def yahoo(self):
        self.df['Close'] = self.df['Adj Close']

    def simpleProbabilities(self, filterSet=False, price=0, csv = False):
        """Calculates simple probabilities of a given move. Except for priceMove and priceTarget,
            name firstSecond = first - Second, (first *Minus* Second)"""
        if filterSet == False:
            filterSet = self.filterSet
        else:
            filterSet = filterSet
        priceMove = []
        priceTarget = []
        highOpen = []
        lowOpen = []
        bothFromOpen = []
        closeOpenGreater = []
        closeOpenLess = []
        openPrevCloseGreater = []
        openPrevCloseLess = []
        highPrevClose = []
        lowPrevClose = []
        bothFromPrevClose = []
        closePrevCloseGreater = []
        closePrevCloseLess = []
        dfLength = len(self.df)
        for x in filterSet:
            priceMove.append(x*price)
            priceTarget.append((x*price)+price)
            highOpen.append(len(self.df[self.df.highOpen > x]) / dfLength)
            lowOpen.append(len(self.df[self.df.lowOpen < x]) / dfLength)
            bothFromOpen.append(len(self.df[(self.df.highOpen > x) & (self.df.lowOpenAbs > x)]) / dfLength)
            closeOpenGreater.append(len(self.df[self.df.closeOpen > x]) / dfLength)
            closeOpenLess.append(len(self.df[self.df.closeOpen < x]) / dfLength)
            openPrevCloseGreater.append(len(self.df[self.df.openPrevClose > x]) / dfLength)
            openPrevCloseLess.append(len(self.df[self.df.openPrevClose < x]) / dfLength)
            highPrevClose.append(len(self.df[self.df.highPrevClose > x]) / dfLength)
            lowPrevClose.append(len(self.df[self.df.lowPrevClose < x]) / dfLength)
            bothFromPrevClose.append(len(self.df[(self.df.highPrevClose > x) & (self.df.lowPrevCloseAbs > x)]) / dfLength)
            closePrevCloseGreater.append(len(self.df[self.df.closePrevClose > x]) / dfLength)
            closePrevCloseLess.append(len(self.df[self.df.closePrevClose < x]) / dfLength)
        dfCols = {'filterSet':filterSet, 'priceMove':priceMove, 'priceTarget':priceTarget, 'highOpen':highOpen,
                  'lowOpen':lowOpen, 'bothOpen':bothFromOpen, 'closeOpenGreater':closeOpenGreater,
                  'closeOpenLess':closeOpenLess, 'openPrevCloseGreater':openPrevCloseGreater,
                  'openPrevCloseLess':openPrevCloseLess, 'highPrevClose':highPrevClose, 'lowPrevClose':lowPrevClose,
                  'bothFromPrevClose':bothFromPrevClose, 'closePrevCloseGreater':closePrevCloseGreater,
                  'closePrevCloseLess':closePrevCloseLess}
        self.dfSimpleProbabilities = pd.DataFrame.from_dict(dfCols)

        if csv != False:
            self.dfSimpleProbabilities.to_csv(csv)
        print('WARNING: Results have not yet been verified. Please verify and remove this print statement.')
        print('Function assumes data is formatted Oldest to Newest. Please ensure this is so before continuing.')
        print('Simple probablities of a give move. Returns df of resutls. If csv = path, outputs to csv.')

        return self.dfSimpleProbabilities


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