import pandas as pd

df = pd.read_csv(,
                 encoding = "ISO-8859-15")

#Totals
print('Account Totals')
grosspl = round(sum(df['Gross PL']), 2)
netpl = round(sum(df['Net PL']), 2)
netfc = round(sum(df['Fees & Commissions']), 2)
print('Number of Trades = {}'.format(len(df)))
print('Total Gross PL = {}'.format(grosspl))
print('Total Fees & Commissions = {}'.format(round(sum(df['Fees & Commissions']),2)))
print('Total Net PL = {}'.format(netpl))


print("")
print('By Type')
tradeTypes = df['Type'].unique()
for t in tradeTypes:
    byTypes = df[df['Type'] == t]
    typesProfit = round(sum(byTypes['Net PL']), 2)
    print('{} Net PL = {}'.format(t, typesProfit))

#By Trader
trader1 = 
trader2 =
trader3 = 
traders = [trader1, trader2, trader3]
for trader in traders:
    print("")
    print(trader)
    filtered = df[df['Trader'] == trader]
    print('{} Number of Trades = {}'.format(trader, len(filtered)))
    gross = round(sum(filtered['Gross PL']), 2)
    net = round(sum(filtered['Net PL']), 2)
    bigwin = max(filtered['Net PL'])
    print('{} Gross Profit/Loss = {}'.format(trader, gross))
    print('{} Total Fees & Commissions = {}'.format(trader, round(sum(filtered['Fees & Commissions']),2)))
    print('{} Net Profit/Loss = {}'.format(trader, net))
    print('{} Average Trade PL = {}'.format(trader, round(filtered['Net PL'].mean(),2)))
    print('{} Biggest Win = {}'.format(trader, round(max(filtered['Net PL']), 2)))
    print('{} Biggest Loss = {}'.format(trader, round(min(filtered['Net PL']), 2)))

    print("")
    print("By Type")
    types = filtered['Type'].unique()
    for x in types:
        byType = filtered[filtered['Type'] == x]
        typeProfit = round(sum(byType['Net PL']), 2)
        print('{} {} Net Profit/Loss = {}'.format(trader, x, typeProfit))


    print("")
    print("By Ticker")
    tickers = filtered['Ticker'].unique()
    for y in tickers:
        byTicker = filtered[filtered['Ticker'] == y]
        tickerProfit = round(sum(byTicker['Net PL']), 2)
        print('{} {} Net Profit/Loss = {}'.format(trader, y, tickerProfit))


        
            
                      
        
                
    

