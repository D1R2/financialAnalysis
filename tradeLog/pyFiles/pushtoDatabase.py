import sqlite3

file = r"C:\Users\David\Desktop\gitHome\trading\tradeLogPrivate\excelFiles\AccountStatementExports\2017-06-19-dayOnly.csv"


db_conn = sqlite3.connect(r"C:\Users\David\Desktop\gitHome\tradingPrivate\tradeLogPrivate\dbFiles\test.db")
db_conn.cursor()


##db_conn.execute("""CREATE TABLE rawTransactions(Date STRING Time STRING Type STRING RefNumber
##                INTEGER Description STRING MiscFees REAL Commissions REAL Amount REAL Balance REAL)""")

#db_conn.execute("""CREATE TABLE cleanTransactions(Date STRING Time STRING Desciption STRING FeesAndCommissions REAL Amount REAL)""")

db_conn.commit()



db_conn.close()
