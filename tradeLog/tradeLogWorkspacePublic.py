import pandas as pd
from tradeLogMaster import transactions
from tradeLogMaster import trades


file = r"C:\Users\David\Desktop\gitHome\tradingPublic\tradeLog\sampleTradesCSV.csv"
database = [r"C:\Users\David\Desktop\gitHome\tradingPublic\tradeLog\dbFiles\placeholder.txt"]

trades(file, database)


