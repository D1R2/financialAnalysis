import pandas as pd
import sqlite3

def transactions(csvPath, databasePaths, destination):
    # Import csv to DF:
    df = pd.read_csv(csvPath)
    df.columns = ['Date', 'Time', 'Type', 'Reference', 'Description', 'MiscFees', 'Commissions', 'Amount', 'Balance']

    # Clean and Output:
    dfClean = df.copy()
    dfClean['feesAndCommissions'] = dfClean['MiscFees'] + dfClean['Commissions']
    dfClean = dfClean[(dfClean['Type'] == 'TRD') | (dfClean['Type'] == 'RAD')]
    dfClean = dfClean[['Date', 'Time', 'Description', 'feesAndCommissions', 'Amount']]
    dfClean.to_csv(destination, index=False)

    # Store in Master and Backup Database:
    for x in databasePaths:
        conn = sqlite3.connect(x)
        conn.execute("""CREATE TABLE IF NOT EXISTS transactions (Date TEXT, Time TEXT, Type TEXT, Reference INTEGER, Description TEXT,
                        MiscFees REAL, Commissions REAL, Amount REAL, Balance REAL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS cleanTransactions (Date TEXT, Time TEXT, Description TEXT,
                                feesAndCommissions REAL, Amount REAL)""")
        df.to_sql('transactions', conn, index=False, if_exists='append')
        dfClean.to_sql('cleanTransactions', conn, index=False, if_exists='append')
        conn.commit()
        conn.close()
    return df

class trade:
    def __init__(self):
       self.trader = None #
       self.types = None #
       self.tickers = None #
       self.notes = None #
       self.optionType = None #
       self.expectedRisk = None #
       self.maxRisk = None #
       self.description = None #
       self.dateOpen = None #
       self.dateClose = None #
       self.timeOpen = None #
       self.timeClose = None #
       self.feesAndCommissions = None #
       self.grossPL = None #
       self.netPL = None #
       self.returnOnExpectedRisk = None #
       self.returnOnMaxRisk = None #
       self.transactions = None #
       self.trade = None #
       
        
    def inputs(self, trader=None, types=None, tickers=None, notes=None, expectedRisk=None, maxRisk=None, optionType=None):
        self.trader = trader
        self.types = types
        self.tickers = tickers
        self.notes = notes
        self.expectedRisk = expectedRisk
        self.maxRisk = maxRisk
        self.optionType = optionType
        
    
    def transactions(self, transactionList):
        self.transactions = []
        for x in transactionList:
            self.transactions += x

    def close(self, databaseList, toCsv = False):
        #Set Final Variables
        df = pd.DataFrame(self.transactions)
        df.columns = ['Date', 'Time', 'Description', 'feesAndCommissions', 'Amount']
        self.description = []
        for x in df:
            self.description.append(df['Description'])
        self.dateOpen = df['Date'].iloc[0]
        self.dateClose = df['Date'].iloc[-1]
        self.timeOpen = df['Date'].iloc[0]
        self.timeClose = df['Date'].iloc[-1]
        self.feesAndCommissions = sum(df['feesAndCommissions'])
        self.grossPL = sum(df['Amount'])
        self.netPL = self.feesAndCommissions + self.grossPL
        self.returnOnExpectedRisk = self.netPL / self.expectedRisk
        self.returnOnMaxRisk = self.netPL / self.maxRisk
        
        trade = [self.trader, self.types, self.tickers, self.notes, self.expectedRisk, self.maxRisk, self.optionType,
                 self.description, self.datOpen, self.dateClose, self.timeOpen, self.timeClose, self.feesAndCommissions,
                 self.grossPL, self.netPL, self.returnOnExpectedRisk, self.returnOnMaxRisk]
        
        #Add to Databases:
        for x in databaseList:
            conn = sqlite3.connect(x)
            conn.execute("""CREATE TABLE IF NOT EXISTS fullTrades (Status TEXT, Date TEXT, Time TEXT, Description TEXT,
                    FeesAndCommissions REAL, Amount REAL)""")
            conn.commit()
            tradeList.to_sql('fullTrades', conn, index = False, if_exists='append')
            conn.close()



        
