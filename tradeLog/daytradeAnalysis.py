import pandas as pd

df = pd.read_csv(r"C:\Users\David\Desktop\gitHome\trading\tradeResults2.csv")

filtered = df[df['Trader'] == 'DAVID']
dayTrades = filtered[filtered['Type'] == 'DAYTRADE']

dayTrades['Percent PL'] = dayTrades['Net PL'] / dayTrades['Open Cost'] * (-1)

print(round(sum(dayTrades['Percent PL']), 2))
