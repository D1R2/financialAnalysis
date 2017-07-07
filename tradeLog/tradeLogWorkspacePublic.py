import pandas as pd
import sqlite3
from tradeLogMaster import transactions
from tradeLogMaster import trade
from tradeLogMaster import processTradeQueue
from tradeLogMaster import sqlToDataFrame
from tradeLogMaster import replaceTable

testing = True
runTransactions = False
runTradeQueue = False #Be sure to include "END" as the last line under 'TRADER' in the trade queue.
export = False
replace = False

if testing == True:
    databasePaths = []
else:
    databasePaths = []

if runTransactions == True:
    filePath =
    transactions(filePath, databasePaths,)
    print('Raw Transactions processed into clean transactions. Please move Account Statement into Account Statement folder.')
    print('Please re-set Workspace parameters.')

if runTradeQueue == True:
    tradeQueue =
    if testing == True:
        processTradeQueue(tradeQueue, databasePaths)
    else:
        processTradeQueue(tradeQueue, databasePaths, clearQueue=True)
    print('Please re-set Workspace parameters.')

if export == True:
    databasePath =
    tableName =
    df = sqlToDataFrame(databasePath,tableName)
    df.to_csv()

if replace == True:
    replaceTable()

