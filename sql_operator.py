
from logging import basicConfig
import sqlite3
from sqlite3.dbapi2 import IntegrityError
from sql_connector import SQLite3Connector

class SQLite3Operator:
  def __init__(self, sqlite_connector: SQLite3Connector) -> None:
    self.connector = sqlite_connector

  # ================ basic op fields ================
  # add delete modify query
  def Commit(self):
    if self.connector.conn != None:
      self.connector.conn.commit()
  
  def InsertDictToTable(self, d, table_name):
    if self.connector.conn == None:
      return
    insert_sql = "INSERT INTO {} (".format(table_name)
    insert_col = []
    insert_data = []
    field_name_dict = self.connector.structure.table_name_dict[table_name].field_name_dict
    for k, v in d.items():
      field_instance = field_name_dict[k]
      insert_col.append(k)
      insert_data.append(field_instance.ParseToSQLData(v))
    insert_sql += ",".join(insert_col)
    insert_sql += ")\nVALUES ("
    insert_sql += ",".join(["?" for _ in range(len(insert_data))])
    insert_sql += ");"
    self.connector.conn.execute(insert_sql, insert_data)

  def DeleteFromTableByCondition(self, table_name, condition):
    if self.connector.conn == None:
      return
    delete_sql = "DELETE FROM {}".format(table_name)
    if condition is not None:
      delete_sql += " WHERE {}".format(condition)
    delete_sql += ";"
    self.connector.conn.execute(delete_sql)

  def UpdateFieldFromTable(self, field_dict, table_name, condition):
    if self.connector.conn == None:
      return
    update_sql = "UPDATE {} SET ".format(table_name)
    field_names = []
    field_datas = []
    field_name_dict = self.connector.structure.table_name_dict[table_name].field_name_dict
    for key, value in field_dict.items():
      field_instance = field_name_dict[key]
      field_names.append(key)
      field_datas.append(field_instance.ParseToSQLData(value))
    update_sql += ",".join(map(lambda x: "{}=?".format(x), field_names))
    if condition is not None:
      update_sql += " WHERE {}".format(condition)
    update_sql += ";"
    self.connector.conn.execute(update_sql, field_datas)

  def SelectFieldFromTable(self, fields, table_name, condition=None):
    if isinstance(fields, (list, tuple)):
      fields = ",".join(fields)
    table_fields = self.connector.structure.table_name_dict[table_name].fields
    field_name_dict = self.connector.structure.table_name_dict[table_name].field_name_dict
    
    if fields.strip() == "*":
      fields_name_list = [None for _ in range(len(table_fields))]
      fields_class_list = [None for _ in range(len(table_fields))]
      for field in table_fields:
        fields_name_list[field.whole_idx] = field.name
        fields_class_list[field.whole_idx] = field
    else:
      fields_name_list = list(map(lambda s: s.strip(), fields.split(",")))
      fields_class_list = list(map(lambda s: field_name_dict[s], fields_name_list))
    select_sql = "SELECT {} FROM {}".format(fields, table_name)
    if condition is not None:
      select_sql += "WHERE {}".format(condition)
    select_sql += ";"
    cursor = self.connector.conn.cursor()
    cursor.execute(select_sql)
    result_list = cursor.fetchall()
    final_result = []
    for p in result_list:
      record_result = {}
      for idx, c in enumerate(p):
        record_result[fields_name_list[idx]] = \
            fields_class_list[idx].ParseFromSQLData(c)
      final_result.append(record_result)
    return final_result

if __name__ == "__main__":
  from sql_structure import *

  table_name_initiate_dict = {
    "BasicTable": {
      "field_definition": {
        "id": "INTEGER AUTOINCREMENT",
        "name": "TEXT UNIQUE NOT NULL",
        "time": "REAL"
      },
    },
    "test_table": {
      "field_definition": {
        "id": "INT NOT NULL",
        "id2": "INT",
        "hell": "BLOB"
      },
      "primary_keys": ["id", "hell"]
    }
  }
  db = SQLDatabase.CreateFromDict(table_name_initiate_dict)
  conn = SQLite3Connector("test.db", db)
  conn.Connect()
  conn.TableValidation()
  op = SQLite3Operator(conn)
  op.InsertDictToTable({"name": "ssc"}, "BasicTable")
  try:
    op.InsertDictToTable({"name": "hello"}, "BasicTable")
  except sqlite3.IntegrityError:
    print("integrity error happened")
  op.InsertDictToTable({"name": "awda"}, "BasicTable")
  # op.UpdateFieldFromTable({"name": "aka"}, "BasicTable", "id == 1")
  # r = op.SelectFieldFromTable("*", "BasicTable")
  # print(r)
  op.Commit()
  pass
