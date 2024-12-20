# CC BY-SA, Eric Mossotti
import duckdb
from colorama import Fore, Style

class DuckDBEnumTypeCreator:
  
  def __init__(self, connection):
    """
    Initialize the enum type creator with a database connection.
    
    :param connection: An active DuckDB database connection
    """
    self.conn = connection
  
  def create_type(self, type_str: str, enum_str: str) -> str:
    """
    Generate the SQL statement to create an enum type.
    
    :param type_str: Name of the enum type
    :param enum_str: Enum values in string format (e.g., "('value1', 'value2')")
    :return: SQL CREATE TYPE statement
    """
    return f"CREATE TYPE {type_str} AS ENUM {enum_str};"
  
  def drop_type(self, type_str: str) -> str:
    """
    Generate the SQL statement to create an enum type.
    
    :param type_str: Name of the enum type
    :return: SQL DROP TYPE statement
    """
    return f"DROP TYPE {type_str};"

  def query_execute(self, type_str: str, enum_str: str) -> None:
    """
    Execute the enum type creation.
    
    :param type_str: Name of the enum type
    :param enum_str: Enum values in string format
    """
    return self.conn.execute(self.create_type(type_str, enum_str))
  
  def try_create(self, type_str: str, enum_str: str) -> None:
    """
    Attempt to create an enum type, handling existing type scenarios.
    
    :param type_str: Name of the enum type
    :param enum_str: Enum values in string format
    """
    try:
      self.query_execute(type_str, enum_str)
      print(
           Fore.LIGHTGREEN_EX + f'{type_str}'
           , Style.DIM + f'was created'
           , Style.NORMAL
           , Fore.RESET
           , sep = " "
           )
    except duckdb.duckdb.CatalogException:
      print(
            Fore.LIGHTGREEN_EX + f'{type_str}'
           , Style.BRIGHT + Fore.GREEN + f'already exists'
           , Style.NORMAL
           , Fore.RESET
           , sep = " "
           )
      
  def try_drop(self, type_str: str) -> None:
    try:
      self.conn.execute(self.drop_type(type_str))
      print(
           Fore.LIGHTRED_EX + f'{type_str}'
           , Style.DIM + f'was dropped'
           , Style.NORMAL
           , Fore.RESET
           , sep = " ")
    except duckdb.duckdb.CatalogException:
      print(
           Fore.LIGHTRED_EX + f'{type_str}'
           , Style.BRIGHT + Fore.RED + f'does not exist'
           , Style.NORMAL
           , Fore.RESET
           , sep = " ")
