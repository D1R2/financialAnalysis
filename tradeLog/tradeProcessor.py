import pandas as pd
#Need to change order of columns in the 'Clean Data' portion of the module. 

filepath = r"C:\Users\David\Desktop\gitHome\trading\Book4.csv"
tradeList = pd.read_csv(filepath)
tradeList.fillna(0, inplace=True)

tradeLines = []
thisTrade = []
transactions = []

n = 0

while n < len(tradeList):
    if tradeList['Status'].iloc[n] == 'NEW': 
        if transactions != []:
            #Append Previouse Trade
            df = pd.DataFrame(transactions, columns = ['Date', 'Time', 'Description', 'Fees & Commissions', 'Amount'])
            openDate = df['Date'].iloc[0]
            closeDate = df['Date'].iloc[-1]
            openTime = df['Time'].iloc[0]
            closeTime = df['Time'].iloc[-1]
            description = df['Description'].iloc[0]
            openCost = df['Amount'].iloc[0]+df['Fees & Commissions'].iloc[0]
            fc = sum(df['Fees & Commissions'])
            grosspl = sum(df.Amount)
            netpl = fc + grosspl
            
            results = [openDate, closeDate, openTime, closeTime, description, openCost, fc, grosspl, netpl]
            for x in results:
                thisTrade.append(x)
            tradeLines.append(thisTrade) 
        
            #Reset Variables
            thisTrade = []
            transactions = []
        
        #Append new Trade identifiers
            
        status, trader, tradeType, ticker, subtypes, notes = tradeList.iloc[n]
        inputs = [trader, tradeType, ticker, subtypes, notes]
        for x in inputs:
            thisTrade.append(x)
        
    else: 
        #Add transaction to 'transactions'. 
        status, date, time, description, amount, fc = tradeList.iloc[n]
        fc = float(fc)
        amount = float(amount)
        transaction = [date, time, description, fc, amount]
        transactions.append(transaction)
    n+=1
        
tradeResults = pd.DataFrame(tradeLines, columns = ['Trader', 'Type', 'Ticker', 'Subtypes', 'Notes', 'Open Date', 'Close Date', 
                                                  'Open Time', 'Close Time', 'Description', 'Open Cost', 'Fees & Commissions', 
                                                  'Gross PL', 'Net PL'])
tradeResults.to_csv('tradeResults.csv')
