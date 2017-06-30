import pandas as pd

df = pd.read_csv()

filtered = df[df['Trader'] == []
dayTrades = filtered[filtered['Type'] == 'DAYTRADE']

dayTrades['Percent PL'] = dayTrades['Net PL'] / dayTrades['Open Cost'] * (-1)

print(round(sum(dayTrades['Percent PL']), 2))
