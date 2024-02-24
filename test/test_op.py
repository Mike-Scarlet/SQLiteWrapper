
from SQLiteWrapper import *

test_default_dict = {
  "BasicTable": {
    "field_definition": {
      "id": "INT UNIQUE",
      "name": "TEXT"
    }
  }
}

# db = SQLDatabase.CreateFromDict(table_name_initiate_dict)
db = SQLDatabase.CreateFromDict(test_default_dict)

conn = SQLite3Connector(":memory:", db)
conn.Connect()
conn.TableValidation()

op = SQLite3Operator(conn)
d1 = {
  "id": 2,
  "name": "geg",
}

op.InsertDictToTable(d1, "BasicTable", or_condition="OR REPLACE")
print(op.GetLastInsertRowID())
op.InsertDictToTable(d1, "BasicTable", or_condition="OR IGNORE")
print(op.GetLastInsertRowID())
op.InsertDictToTable(d1, "BasicTable", or_condition="OR REPLACE")
print(op.GetLastInsertRowID())
