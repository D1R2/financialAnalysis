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
    clearTransactions = pd.DataFrame(columns=['DATE', 'TIME', 'TYPE', 'REF #', 'DESCRIPTION', 'Misc Fees', 'Commissions & Fees', 'AMOUNT', 'BALANCE'])
    clearTransactions.to_csv(csvPath, index=False)

class trade:
    def __init__(self):
       self.trader = 'None'
       self.types = 'None'
       self.tickers = 'None'
       self.options = 'None'
       self.expectedRisk = 'None'
       self.maxRisk = 'None'
       self.notes = 'None'
       self.description = ''
       self.dateOpen = 'None'
       self.dateClose = 'None'
       self.timeOpen = 'None'
       self.timeClose = 'None'
       self.feesAndCommissions = 'None'
       self.grossPL = 'None'
       self.netPL = 'None'
       self.returnOnExpectedRisk = 'None'
       self.returnOnMaxRisk = 'None'
       self.transactions = []
       
        
    def inputs(self, trader=None, types=None, tickers=None, options=None, expectedRisk=None, maxRisk=None, notes=None):
        self.trader = trader
        self.types = types
        self.tickers = tickers
        self.notes = notes
        self.expectedRisk = expectedRisk
        self.maxRisk = maxRisk
        self.options = options
        
    
    def addTransaction(self, date, time, description, fc, amount):
        transactionList = [date, time, description, fc, amount]
        self.transactions.append(transactionList)

    def close(self):
        #Set Final Variables
        df = pd.DataFrame(self.transactions)
        df.columns = ['Date', 'Time', 'Description', 'feesAndCommissions', 'Amount']
        for x in df['Description']:
            self.description += '{} ,'.format(x)
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
        
    def save(self, databasePaths):
        for x in databasePaths:
            conn = sqlite3.connect(x)
            conn.execute('''CREATE TABLE IF NOT EXISTS fullTrades (Trader TEXT, Types TEXT, Tickers TEXT, Notes TEXT, Date TEXT, Time TEXT, 
                                                                        Description TEXT, FeesAndCommissions REAL, Amount REAL)''')
            conn.execute('''INSERT INTO fullTrades (Trader, Types, Tickers, Notes) VALUES(?, ?, ?, ?)''',
                         (self.trader, self.types, self.tickers, self.options, self.expectedRisk, self.maxRisk, self.notes,))


            conn.commit()
            tradeList.to_sql('fullTrades', conn, index = False, if_exists='append')
            conn.close()

def processTradeQueue(self, csvPath, databasePaths):
    df = pd.read_csv(csvPath)
    df['TRADER'].fillna('LEG', inplace=True)
    fillZero = ['EXPECTED', 'MAX', 'F&C', 'AMOUNT']
    for z in fillZero:
        df.[z].fillna(0, inplace=True)

    for x in range(len(df)):
        trader, types, tickers, options, expected, max, notes, date, time, description, fc, amount = df.iloc[x]
        if df['TRADER'].iloc[x] != 'LEG':
            try:
                trade.close(databasePaths)
            except NameError:
                pass
            trade = trade()
            trade.inputs(trader, types, tickers, options, expected, max, notes)
        else:
            trade.addTransaction(date, time, description, fc, amount)










        
