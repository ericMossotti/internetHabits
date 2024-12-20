# Python-based regex filtering -----
import duckdb
import re

def filter_columns_by_regex(col_dict: dict, regex_pattern: str) -> dict:
  """
  Filter column dictionary based on a regex pattern
  
  Parameters:
  -----------
  col_dict : dict
      Original column dictionary
  pattern : str
      Regex pattern to match column names
  
  Returns:
  --------
  dict
      Filtered column dictionary
  """
  return {
    col: dtype 
    for col, dtype in col_dict.items() 
    if re.search(regex_pattern, col)
    }

def create_table_with_regex_columns(
  conn: duckdb.duckdb,
  source_table: str, 
  new_table_name: str, 
  regex_pattern: str, 
  col_dict: dict
  ) -> None:  
  """
  Create a new table with columns matching a regex pattern
  
  Parameters:
  -----------
  conn : DuckDB connection
  source_table : str
      Name of the source table
  new_table_name : str
      Name of the new table to create
  col_dict : dict
      Original column dictionary
  regex_pattern : str
      Regex pattern to match column names
  """
  # Filter columns using regex
  filtered_col_dict = filter_columns_by_regex(col_dict, regex_pattern)
  
  # A flexible regex-based column selection in DuckDB
  regex_select_sql = f"""
  CREATE OR REPLACE TABLE {new_table_name} AS 
  SELECT
    {', '.join([f'"{col}"' for col in filtered_col_dict.keys()])}
  FROM {source_table};
  """

  conn.execute(regex_select_sql)
