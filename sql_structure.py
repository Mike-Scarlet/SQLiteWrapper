
"""
SQLBase

base structure to define sqlite database

abstraction
- SQLite database file
  (has) Table
    (has) Field

the SQL structure should be constructed on program loading,
when it should be read only during whole program process
"""

class SQLField:
  UNIQUE_TOKEN = "UNIQUE"
  NOT_NULL_TOKEN = "NOT NULL"
  AUTO_INCREMENT_TOKEN = "AUTOINCREMENT"
  DEFAULT_TOKEN = "DEFAULT"

  """ base sql field impl """
  def __init__(self,
               name: str,
               data_type_str: str,
               unique: bool = False,
               not_null: bool = False,
               auto_increment: bool = False,
               default: any = None) -> None:
    self.name = name
    self.data_type_str = data_type_str
    self.data_class = SQLField.GetClass(self.data_type_str)
    self.unique = unique
    self.not_null = not_null
    self.auto_increment = auto_increment
    self.default = default
    self.whole_idx = None

  def __repr__(self) -> str:
    return "<SQLField: '{}' at {:016X}>".format(self.name, id(self))

  def GetCreateStr(self):
    s = self.data_type_str
    subs = []
    if self.unique:
      subs.append(SQLField.UNIQUE_TOKEN)
    if self.not_null:
      subs.append(SQLField.NOT_NULL_TOKEN)
    if self.default is not None:
      subs.append("DEFAULT {}".format(self.default))
    if self.auto_increment:
      # note: in sqlite, auto increment must be led by primary key
      subs.append("PRIMARY KEY " + SQLField.AUTO_INCREMENT_TOKEN)
    if len(subs) > 0:
      s += " " + " ".join(subs)
    return s

  def ParseToSQLData(self, value):
    if value is None:
      return "NULL"
    elif self.data_class == str:
      return value
    elif self.data_class in (int, float):
      return str(value)
    elif self.data_class == bytes:
      assert(isinstance(value, bytes))
      return value
    else:
      raise ValueError("not supported data type: {}".format(self.data_class))
  
  def ParseFromSQLData(self, value):
    return value

  @staticmethod
  def GetClass(s):
    s_up = s.upper()
    if s_up == "INT" or s_up == "INTEGER":
      return int
    elif s_up == "TEXT":
      return str
    elif s_up == "REAL":
      return float
    elif s_up == "BLOB":
      return bytes
    else:
      raise ValueError("not supported data type: {}".format(s))

class SQLTable:
  def __init__(self, name: str) -> None:
    self.fields = []
    self.field_name_dict = {}
    self.name = name
    self.primary_keys = []

  def __repr__(self) -> str:
    return "<SQLTable: '{}' at {:016X}>".format(self.name, id(self))

  @staticmethod
  def CreateFromDict(name: str, name_type_dict: dict, primary_keys: list or str=None):
    table = SQLTable(name)
    for field_name, data_type in name_type_dict.items():
      value = data_type

      # take out extra param
      value_up = value.upper()
      test_result = {SQLField.DEFAULT_TOKEN: None}
      for test_token in (SQLField.UNIQUE_TOKEN, SQLField.NOT_NULL_TOKEN, SQLField.AUTO_INCREMENT_TOKEN):
        if test_token in value_up:
          start_idx = value_up.find(test_token)
          value_up = value_up[:start_idx] + value_up[start_idx + len(test_token):]
          value = value[:start_idx] + value[start_idx + len(test_token):]
          test_result[test_token] = True
        else:
          test_result[test_token] = False
      # parse DEFAULT
      if SQLField.DEFAULT_TOKEN in value_up:
        start_idx = value_up.find(SQLField.DEFAULT_TOKEN)
        # get default value
        default_value = value[start_idx + len(SQLField.DEFAULT_TOKEN):].strip().split(" ")[0]
        test_result[SQLField.DEFAULT_TOKEN] = default_value
      
      field_type = value.strip().split(" ")
      if len(field_type) <= 0:
        raise ValueError("invalid field type {}".format(field_type))
      field = SQLField(field_name, field_type[0],
                       test_result[SQLField.UNIQUE_TOKEN], test_result[SQLField.NOT_NULL_TOKEN], 
                       test_result[SQLField.AUTO_INCREMENT_TOKEN], test_result[SQLField.DEFAULT_TOKEN])
      table.fields.append(field)
      table.field_name_dict[field_name] = field

    if primary_keys is None:
      pass
    elif isinstance(primary_keys, str):
      primary_keys = [primary_keys]
    elif isinstance(primary_keys, list) and len(primary_keys) == 0:
      primary_keys = None
    table.primary_keys = primary_keys

    # test only one primary key exist
    primary_key_count = 0
    if table.primary_keys is not None:
      primary_key_count += 1
    for field in table.fields:
      if field.auto_increment:
        primary_key_count += 1
    if primary_key_count > 1:
      raise ValueError("multiple primary key found in table: {}".format(table.name))
    
    return table

class SQLDatabase:
  def __init__(self) -> None:
    self.tables = []
    self.table_name_dict = {}

  def __repr__(self) -> str:
    return "<SQLDatabase at {:016X}>".format(id(self))

  @staticmethod
  def CreateFromDict(table_name_initiate_dict: dict):
    db = SQLDatabase()
    for table_name, initiate_dict in table_name_initiate_dict.items():
      if "field_definition" not in initiate_dict:
        raise ValueError("add field_definition to table: {}".format(table_name))
      table = SQLTable.CreateFromDict(table_name, 
                                      initiate_dict["field_definition"], 
                                      initiate_dict.get("primary_keys", None))
      db.table_name_dict[table_name] = table
      db.tables.append(table)
    return db

if __name__ == "__main__":
  table_name_initiate_dict = {
    "BasicTable": {
      "field_definition": {
        "id": "INTEGER NOT NULL AUTOINCREMENT",
        "name": "TEXT",
        "time": "REAL"
      },
      "primary_keys": "id"
    },
    "test_table": {
      "field_definition": {
        "id": "INT NOT NULL",
        "hell": "BLOB"
      }
    }
  }
  test_default_dict = {
    "BasicTable": {
      "field_definition": {
        "id": "INT DEFAULT 2",
        "name": "TEXT"
      }
    }
  }

  # db = SQLDatabase.CreateFromDict(table_name_initiate_dict)
  db = SQLDatabase.CreateFromDict(test_default_dict)
  pass