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
        self.df = df.copy()
        if yahoo==True:
            self.adjClose()
        self.filterSet =[-.05, -.045, -.04, -.035, -.03, -.025, -.02, -.0175, -.015, -.0125, -.01,
                     -.009, -.008, -.007, -.006, -.005, -.004, -.003, -.002, -.001, 0, .001,
                     .002, .003, .004, .005, .006, .007, .008, .009, .01, .0125, .015, .0175, .02,
                     .025, .03, .035, .04, .045, .05]
        self.dfSimpleProbabilities= None

    def adjClose(self):
        '''Sets close equal to Adjusted Close'''
        self.df['Close'] = self.df['Adj Close']

    def resample(self, frequency):
        df = self.df
        periods = len(df)
        index = pd.date_range('01,01,2000', periods=periods, freq='T')
        df.index = index

        freq = '{}T'.format(frequency)
        dfFirst = df.resample(freq).first()
        dfLast = df.resample(freq).last()
        dfMax = df.resample(freq).max()
        dfMin = df.resample(freq).min()
        dfSum = df.resample(freq).sum()

        dfResampled = pd.DataFrame()
        dfResampled['Date'] = dfFirst.Date
        dfResampled['Open'] = dfFirst.Open
        dfResampled['High'] = dfMax.High
        dfResampled['Low'] = dfMin.Low
        dfResampled['Close'] = dfLast.Close
        dfResampled['Volume'] = dfSum.Volume

        self.df = dfResampled



    def differenceCols(self):
        '''Sets columns for various standard column differences in percentage terms.'''
        self.df['highOpen'] = (self.df.High - self.df.Open) / self.df.Open
        self.df['lowOpen'] = (self.df.Low - self.df.Open) / self.df.Open
        self.df['lowOpenAbs'] = abs(self.df.lowOpen)
        self.df['closeOpen'] = (self.df.Close - self.df.Open) / self.df.Open
        self.df['openPrevClose'] = (self.df.Open.shift(-1) - self.df.Close) / self.df.Close
        self.df['highPrevClose'] = (self.df.High.shift(-1) - self.df.Close) / self.df.Close
        self.df['lowPrevClose'] = (self.df.Low.shift(-1) - self.df.Close) / self.df.Close
        self.df['lowPrevCloseAbs'] = abs(self.df.lowPrevClose)
        self.df['closePrevClose'] = (self.df.Close.shift(-1) - self.df.Close) / self.df.Close
        self.df['highLow'] = (self.df.High - self.df.Low) / self.df.Open

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
        neitherFromOpen = []
        eitherFromOpen = []
        closeOpenGreater = []
        closeOpenLess = []
        openPrevCloseGreater = []
        openPrevCloseLess = []
        highPrevClose = []
        lowPrevClose = []
        bothFromPrevClose = []
        neitherFromPrevClose = []
        eitherFromPrevClose = []
        closePrevCloseGreater = []
        closePrevCloseLess = []
        dfLength = len(self.df)
        for x in filterSet:
            priceMove.append(x*price)
            priceTarget.append((x*price)+price)
            highOpen.append(len(self.df[self.df.highOpen > x]) / dfLength)
            lowOpen.append(len(self.df[self.df.lowOpen < x]) / dfLength)
            bothFromOpen.append(len(self.df[(self.df.highOpen > x) & (self.df.lowOpenAbs > x)]) / dfLength)
            eitherFromOpen.append(len(self.df[(self.df.highOpen > x) | (self.df.lowOpenAbs >x)]) / dfLength)
            neitherFromOpen.append(len(self.df[(self.df.highOpen < x) & (self.df.lowOpenAbs < x)]) / dfLength)
            closeOpenGreater.append(len(self.df[self.df.closeOpen > x]) / dfLength)
            closeOpenLess.append(len(self.df[self.df.closeOpen < x]) / dfLength)
            openPrevCloseGreater.append(len(self.df[self.df.openPrevClose > x]) / dfLength)
            openPrevCloseLess.append(len(self.df[self.df.openPrevClose < x]) / dfLength)
            highPrevClose.append(len(self.df[self.df.highPrevClose > x]) / dfLength)
            lowPrevClose.append(len(self.df[self.df.lowPrevClose < x]) / dfLength)
            bothFromPrevClose.append(len(self.df[(self.df.highPrevClose > x) & (self.df.lowPrevCloseAbs > x)]) / dfLength)
            eitherFromPrevClose.append(len(self.df[(self.df.highPrevClose > x) | (self.df.lowPrevCloseAbs > x)]) / dfLength)
            neitherFromPrevClose.append(len(self.df[(self.df.highPrevClose < x) & (self.df.lowPrevCloseAbs < x)]) / dfLength)
            closePrevCloseGreater.append(len(self.df[self.df.closePrevClose > x]) / dfLength)
            closePrevCloseLess.append(len(self.df[self.df.closePrevClose < x]) / dfLength)
        dfCols = {'a-Filter Set':filterSet, 'b-Price Move':priceMove, 'c-Prive Target':priceTarget, 'd-highOpen':highOpen,
                  'e-lowOpen':lowOpen, 'f-bothOpen':bothFromOpen, 'g-eitherFromOpen':eitherFromOpen, 'h-closeOpenGreater':closeOpenGreater,
                  'i-closeOpenLess':closeOpenLess, 'j-openPrevCloseGreater':openPrevCloseGreater,
                  'k-openPrevCloseLess':openPrevCloseLess, 'l-highPrevClose':highPrevClose, 'm-lowPrevClose':lowPrevClose,
                  'n-bothFromPrevClose':bothFromPrevClose, 'o-eitherFromPrevClose':eitherFromPrevClose, 'p - closePrevCloseGreater':closePrevCloseGreater,
                  'q - closePrevCloseLess':closePrevCloseLess, 'r-neitherFromOpen':neitherFromOpen,
                  's-neitherFromPrevClose':neitherFromPrevClose}
        self.dfSimpleProbabilities = pd.DataFrame.from_dict(dfCols)

        if csv != False:
            self.dfSimpleProbabilities.to_csv(csv, index=False)

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