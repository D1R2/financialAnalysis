#Trade Logging Tool

import pandas as pd
import sqlite3

file =r"C:\Users\David\Desktop\cleanData.csv"

class data: 
    def dataPrep(filePath):
        df = pd.read_csv(filePath)
        df['Fees and Commissions'] = df['Misc Fees'] + df['Commissions & Fees']
        df = df[(df['TYPE'] == 'TRD') | (df['TYPE'] == 'RAD')]
        delCols = ['Misc Fees', 'Commissions & Fees', 'BALANCE']
        for x in delCols:
            del df[x]
        df.to_csv(r'C:\Users\David\Desktop\cleanData2.csv')
        return df
    def addTrades(self, filePath):
        
    
        

class trade:
    df = pd.DataFrame()
    def inputs(self, trader, tradeType, tickers, subtypes):
        self.trader = trader
        self.tradeType = tradeType
        self.tickers = tickers
        self.subtypes = subtypes
    def addLeg(self, string):

        

    def close(self, df)
        
    
    
    
    




    
