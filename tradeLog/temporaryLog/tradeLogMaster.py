#Trade Logging Tool

import pandas as pd
import sqlite3

def transactions(csvPath, databasePaths, destination): 

    #Import csv to DF:
    df = pd.read_csv(csvPath)
    df.columns = ['Date', 'Time', 'Type', 'Reference', 'Description', 'MiscFees', 'Commissions', 'Amount', 'Balance']
    
    #Store in Master and Backup Database:
    for x in databasePaths:
        conn = sqlite3.connect(x)
        conn.execute("""CREATE TABLE IF NOT EXISTS transactions (Date TEXT, Time TEXT, Type TEXT, Reference INTEGER, Description TEXT,
                    MiscFees REAL, Commissions REAL, Amount REAL, Balance REAL)""")
        conn.commit()
        df.to_sql('transactions', conn, index = False, if_exists='append')
        conn.commit()
        conn.close()
    
    #Clean and Output:
    
    df['FEES & COMMISSIONS'] = df['MiscFees'] + df['Commissions']
    df = df[(df['Type'] == 'TRD') | (df['Type'] == 'RAD')]
    df = df[['Date', 'Time', 'Description', 'Commissions', 'Amount']]
    df.to_csv(destination, index = False)
    return df

def trades(csvPath, databasePaths):
    #Import csv to DF:
    tradeList = pd.read_csv(csvPath)
    tradeList.fillna(0, inplace=True)

    #Save full trades to fullTrades Database:
    for x in databasePaths:
        conn = sqlite3.connect(x)
        conn.execute("""CREATE TABLE IF NOT EXISTS fullTrades (Status TEXT, Date TEXT, Time TEXT, Description TEXT,
                    FeesAndCommissions REAL, Amount REAL)""")
        conn.commit()
        tradeList.to_sql('fullTrades', conn, index = False, if_exists='append')
        conn.close()

    #Process into Trade Summary Lines -- Need to add risk to manual inputs. 
    tradeLines = []
    transactions = []
    tradeDescription = []
    for n in range(len(tradeList)):
            if tradeList['Status'].iloc[n] == 'NEW': 
                if transactions != []:
                    #Append Previouse Trade
                    description = tradeDescription
                    df = pd.DataFrame(transactions, columns = ['Date', 'Time', 'FeesAndCommissions', 'Amount'])
                    openDate = df['Date'].iloc[0]
                    closeDate = df['Date'].iloc[-1]
                    openTime = df['Time'].iloc[0]
                    closeTime = df['Time'].iloc[-1]
                    fc = sum(df['FeesAndCommissions'])
                    grosspl = sum(df['Amount'])
                    netpl = fc + grosspl
                    
                    
                    thisTrade = [trader, tradeType, ticker, description, subtypes, notes, risk, 
                                  openDate, closeDate, openTime, closeTime, fc, grosspl, netpl]
                    tradeLines.append(thisTrade)
                
                    #Reset Variables
                    transactions = []
                    tradeDescription = []
                
                #Set new trade identifiers
                    
                status, trader, tradeType, ticker, subtypes, notes = tradeList.iloc[n]
                risk = tradeList['Status'].iloc[n+1]
                
            else: 
                #Add transaction to 'transactions'. 
                status, date, time, description, amount, fc = tradeList.iloc[n]
                fc = float(fc)
                amount = float(amount)
                transaction = [date, time, fc, amount]
                transactions.append(transaction)
                if tradeDescription == []:
                    tradeDescription += description
                else:
                    tradeDescription += ', {}'.format(description)
    df = pd.DataFrame(tradeLines)
    df.columns = ['Trader', 'Type', 'Tickers', 'Description', 'Subtypes', 'Notes', 'Risk', 'openDate', 'closeDate',
                  'openTime', 'closeTime', 'FeesAndCommissions','grossPL', 'netPL']
    df.to_csv('tradeSummaries.csv')
    
##    for x in databasePaths:
##        df = pd.DataFrame(tradeLines)
##        df.columns = ['Trader', 'Type', 'Tickers', 'Description', 'Subtypes', 'Notes', 'Risk', 'openDate', 'closeDate',
##                       'openTime', 'closeTime', 'FeesAndCommissions','grossPL', 'netPL']
##        print(df.head())
##        conn = sqlite3.connect(x)
##        conn.execute("""CREATE TABLE IF NOT EXISTS tradeSummaries ( Trader TEXT, Type TEXT, Tickers TEXT, Description TEXT, Subtypes TEXT, Notes TEXT, Risk REAL,
##                    openDate TEXT, closeDate TEXT, openTime TEXT, closeTime TEXT, FeesAndCommissions REAL, grossPL REAL, netPL REAL)""")
##        conn.commit()
##        df.to_sql('tradeSummaries', conn, index = False, if_exists='append')
##        conn.commit()
##        conn.close()
        
        
              
        
        
    
    
