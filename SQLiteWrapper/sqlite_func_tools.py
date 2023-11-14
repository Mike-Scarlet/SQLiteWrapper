
__all__ = [
  "ClassToSQLiteFieldDefinition",
  "ClassAndPrimaryKeyToTableInitiateDict",
]

from sqlite_structure import SQLField

def ClassToSQLiteFieldDefinition(class_type):
  """ this function will initiate this class, use at your own risk """
  field_definition_dict = {}
  temp_obj = class_type()
  for name, python_type in class_type.__annotations__.items():
    field_str = SQLField.GetSQLTypeFromPythonType(python_type)
    if isinstance(getattr(temp_obj, name), python_type) and python_type != str:   # string is not enabled
      field_str += " DEFAULT {}".format(getattr(temp_obj, name))
    field_definition_dict[name] = field_str
  return field_definition_dict

def ClassAndPrimaryKeyToTableInitiateDict(class_type, table_name, primary_keys=None):
  db_initiate_dict = {table_name: {"field_definition": {}}}
  db_initiate_dict[table_name]["field_definition"] = ClassToSQLiteFieldDefinition(class_type)
  if primary_keys is not None:
    db_initiate_dict[table_name]["primary_keys"] = primary_keys
  return db_initiate_dict