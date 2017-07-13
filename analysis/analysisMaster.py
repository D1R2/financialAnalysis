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


def openDev(df, filterSet=False):
    # Calculate the simple probability of a given deviation from the open price.
    # Goal is to return a df with probabilities of given move above or below open in percentage terms.

    if filterSet == False:
        filterSet = [-.05, -.045, -.04, -.035, -.03, -.025, -.02, -.0175, -.015, -.0125, -.01
                     - .009, -.008, -.007, -.006, -.005, -.004, -.003, -.002, -.001, 0, .001,
                     .002, .003, .004, .005, .006, .007, .008, .009, .01, .0125, .015, .0175, .02,
                     .025, .03, .035, .04, .045, .05]
    else:
        pass

    df['highDev'] = (df.High - df.Open) / df.Open
    df['lowDev'] = (df.Low - df.Open) / df.Open
    df['lowDevAbs'] = abs(df.lowDev)
    df['closeDev'] = (df.Close - df.Open) / df.Open

    dfOpenDev = pd.DataFrame(index=filterSet)
    highDevCol = []
    lowDevCol = []
    bothDevCol = []
    closeDevGreater = []
    closeDevLess = []

    for x in filterSet:
        highDevCol.append(len(df[df.highDev > x]) / len(df))
        lowDevCol.append(len(df[df.lowDev < x]) / len(df))
        closeDevGreater.append(len(df[df.closeDev > x]) / len(df))
        closeDevLess.append(len(df[df.closeDev < x]) / len(df))
        dfFiltered = df[df.highDev > x]
        dfFiltered = dfFiltered[dfFiltered.lowDevAbs > x]
        bothDevCol.append(len(dfFiltered) / len(df))

    dfOpenDev['highDev'] = highDevCol
    dfOpenDev['lowDev'] = lowDevCol
    dfOpenDev['bothDev'] = bothDevCol
    dfOpenDev['closeDevGreater'] = closeDevGreater
    dfOpenDev['closeDevLess'] = closeDevLess

    return dfOpenDev