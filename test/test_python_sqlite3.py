
from SQLiteWrapper import *
import sqlite3

# cursor = sqlite3.Cursor()

db_path = r"D:\DevelopEnvironment\DataBases\tsm\baostock\company_detail\sh.600004.db"

conn = SQLite3Connector(db_path, None)
conn.Connect()
conn.LoadStructureFromDatabase()

op = SQLite3Operator(conn)
sel_result = op.SelectFieldFromTableAdvanced("count(*)", "CompanyDailyDetail")
pass