#Trade Logging Tool

import pandas as pd
import sqlite3

file =r"C:\Users\David\Desktop\gitHome\tradingPrivate\tradeLogPrivate\excelFiles\rawTransactions.csv"

class data:
    def rawTransactions(self, filePath):
##        Steps:
##            - Store Raw Transactions in DB in DB Folder
##            - Store Raw Transactions in DB in Backups
##            - Clean Data and output to csv.

        #Clean and Output
        df = pd.read_csv(filePath)
        df['FEES & COMMISSIONS'] = df['Misc Fees'] + df['Commissions & Fees']
        df = df[(df['TYPE'] == 'TRD') | (df['TYPE'] == 'RAD')]
        df = df[['DATE', 'TIME', 'DESCRIPTION', 'FEES & COMMISSIONS', 'AMOUNT']]
        df.to_csv(r"C:\Users\David\Desktop\gitHome\tradingPrivate\tradeLogPrivate\excelFiles\cleanTransactions.csv",
                  index = False)
        return df
        
    
        

class trade:
    df = pd.DataFrame()
    def inputs(self, trader, tradeType, tickers, subtypes):
        self.trader = trader
        self.tradeType = tradeType
        self.tickers = tickers
        self.subtypes = subtypes


        
    
    
    
    




    
