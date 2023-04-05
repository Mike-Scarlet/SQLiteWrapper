
from sqlite_structure import SQLDatabase, SQLTable, SQLField
import sqlite3
import os
import copy
import logging

class SQLite3Connector:
  def __init__(self, path: str, structure: SQLDatabase, commit_when_leave: bool=True, verbose_level=10) -> None:
    self.structure = copy.deepcopy(structure)
    self.path = path
    self.conn = None
    self.commit_when_leave = commit_when_leave
    self.logger = logging.getLogger("SQLConnector")
    self.verbose_level = verbose_level

  def __getstate__(self):
    return {
      "structure": self.structure,
      "path": self.path,
      "commit_when_leave": self.commit_when_leave
    }

  def __setstate__(self, state):
    self.structure = state["structure"]
    self.path = state["path"]
    self.commit_when_leave = state["commit_when_leave"]
    self.logger = logging.getLogger("SQLConnector")
    self.conn = sqlite3.connect(self.path)
    pass

  def Connect(self, do_check=True, check_same_thread=True) -> None:
    if not os.path.exists(self.path):
      if do_check:
        if SQLite3Connector._ui_interactive_check(
            "No SQL file at database_path: {}, Do you want to create one?".format(self.path),
            "creating: " + self.path):
          self.conn = sqlite3.connect(self.path, check_same_thread=check_same_thread)
      else:
        if self.verbose_level >= 10:
          print("creating new sqlite file at path: {}".format(self.path))
        self.conn = sqlite3.connect(self.path, check_same_thread=check_same_thread)
    else:
      self.conn = sqlite3.connect(self.path, check_same_thread=check_same_thread)

  def LoadStructureFromDatabase(self) -> None:
    if self.conn == None:
      self.logger.warning("[LoadStructureFromDatabase] self.conn is None")
      return
    # get all current table's name
    cursor = self.conn.cursor()
    cursor.execute("select name from sqlite_master where type='table' order by name")
    names = cursor.fetchall()
    current_table_names = [k for i in names for k in i]

    self.structure = SQLDatabase()
    for table_name in current_table_names:
      if table_name == "sqlite_sequence":
        continue
      field_name_dict = self._GetFieldsDetailForTable(table_name)
      table = SQLTable(table_name)
      table.fields = list(field_name_dict.values())
      table.field_name_dict = field_name_dict
      self.structure.tables.append(table)
      self.structure.table_name_dict[table.name] = table

  def TableValidation(self) -> None:
    if self.conn == None:
      self.logger.warning("[TableValidation] self.conn is None")
      return
    # get all current table's name
    cursor = self.conn.cursor()
    cursor.execute("select name from sqlite_master where type='table' order by name")
    names = cursor.fetchall()
    current_table_names = [k for i in names for k in i]
    # loop required table_names
    for table_name in self.structure.table_name_dict.keys():
      if table_name in current_table_names:
        self._CheckAndAddTableFields(table_name)
      else:
        self._CreateTable(table_name)

  def AddTable(self, table: SQLTable) -> None:
    if self.conn == None:
      self.logger.warning("[AddTable] self.conn is None")
      return
    if table.name in self.structure.table_name_dict:
      raise RuntimeError("trying to add exist table: {}".format(table.name))
    self.structure.table_name_dict[table.name] = table
    self.structure.tables.append(table)
    self._CreateTable(table.name)

  def _CheckAndAddTableFields(self, table_name: str) -> None:
    field_with_idx = self._GetFieldsWithIndexForTable(table_name)
    for field in self.structure.table_name_dict[table_name].fields:
      if field.name not in field_with_idx:
        self.logger.warning("{} not found in {}, creating..".format(field.name, table_name))
        self.conn.execute("alter table {} add column {} {}".format(
                           table_name, field.name, field.GetCreateStr()))
    # refresh field key index
    field_with_idx = self._GetFieldsWithIndexForTable(table_name)
    for field in self.structure.table_name_dict[table_name].fields:
      field.whole_idx = field_with_idx[field.name]

  def _CreateTable(self, table_name: str) -> None:
    # create table
    create_sql = "CREATE TABLE " + table_name + " (\n"
    for field in self.structure.table_name_dict[table_name].fields:
      create_sql += " ".join([field.name, field.GetCreateStr()]) + ',\n'
    if self.structure.table_name_dict[table_name].primary_keys is not None and \
       len(self.structure.table_name_dict[table_name].primary_keys) != 0:
      create_sql += "PRIMARY KEY ({})\t\t".format(",".join(self.structure.table_name_dict[table_name].primary_keys))
    create_sql = create_sql[:-2]
    create_sql += "\n);"
    self.conn.execute(create_sql)
    # refresh field key index
    field_with_idx = self._GetFieldsWithIndexForTable(table_name)
    for field in self.structure.table_name_dict[table_name].fields:
      field.whole_idx = field_with_idx[field.name]

  def _GetFieldsWithIndexForTable(self, table_name: str) -> dict:
    cursor = self.conn.cursor()
    cursor.execute("PRAGMA table_info(%s)" % (table_name))
    table_info = cursor.fetchall()
    result_dict = {}
    for idx, p in enumerate(table_info):
      field_name = p[1]
      field_index = p[0]
      result_dict[field_name] = field_index
    return result_dict

  def _GetFieldsDetailForTable(self, table_name: str) -> dict:
    cursor = self.conn.cursor()
    cursor.execute("PRAGMA table_info(%s)" % (table_name))
    table_info = cursor.fetchall()
    result_dict = {}
    for idx, p in enumerate(table_info):
      field_name = p[1]
      field_index = p[0]
      field_type = p[2]
      field_not_null = p[3]
      field_default_value = p[4]
      field_pk = p[5]
      field = SQLField(field_name, field_type, not_null=field_not_null)
      field.whole_idx = field_index
      result_dict[field_name] = field
    return result_dict

  @staticmethod
  def _ui_interactive_check(enter_msg, success_msg, fail_msg="closing...", instant_exit=True) -> bool:
    print(enter_msg, "(y/n)")
    k = input()
    if not (len(k) == 1 and k.lower() == 'y'):
      print(fail_msg)
      if instant_exit:
        exit()
      else:
        return False
    else:
      print(success_msg)
      return True

  def __del__(self):
    if self.commit_when_leave and self.conn != None:
      self.conn.commit()
      self.conn.close()
      self.logger.info("{} commit and exit success".format(self.path))

if __name__ == "__main__":
  table_name_initiate_dict = {
    "BasicTable": {
      "field_definition": {
        "id": "INTEGER AUTOINCREMENT",
        "name": "TEXT NOT NULL",
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
  conn.LoadStructureFromDatabase()
  conn.TableValidation()
  pass