# Eric Mossotti, CC BY-SA

"""
Sets up a DuckDB pipeline for analyzing and modeling a dataset to predict
problematic internet use in children based on their physical activity data.

Parameters:
-----------
csvDict: {key: value},

parquetDict: {key: value}
    Dictionary object containing user's chosen databse table names and 
    corresponding DataFrame or directory path
    
conn: duckdb.DuckDBPyConnection

Operations:
--------
"""
import duckdb
import pandas
from colorama import Fore, Style

def setup_duckdb_pipeline(
  csvDict: dict, 
  parquetDict: dict, 
  conn: duckdb.DuckDBPyConnection) -> None:
  
  # Configure db memory settings
  #conn.execute("SET memory_limit='16GB'")
  # conn.execute("SET temp_directory='./tmp_duckdb';")
 
  #conn.execute(f"SET default_order = 'ASC';")
 # quarterSeries = pd.Series(data = range(1, 5), dtype = str)
  
  #enum_str = tuple(quarterSeries)
  #conn.execute(f"CREATE TYPE quarter_enum AS ENUM {enum_str};")
  
  try:
    {
      table_name: duckdb.sql(f"""
      CREATE OR REPLACE TABLE {table_name} AS 
      SELECT 
        *
      FROM 
        df;
      """, connection = conn) 
      for table_name, df in csvDict.items()
      }
    
    for key, value in csvDict.items():
      result = conn.execute(f"SELECT COUNT(*) FROM {key}").fetchone()
      print(
           Style.BRIGHT + f"Successfully created table:"
           , Style.NORMAL + Fore.LIGHTYELLOW_EX + f"{key},"
           , Style.BRIGHT + Fore.RESET + f"Row count:"
           , Style.NORMAL + Fore.CYAN + f"{result[0]}"
           , Fore.RESET  
        )
        
  except Exception as e:
    print(f"Error loading table: {str(e)}")
    raise
  
  if parquetDict:
    write_datasets(conn, parquetDict)

  
# Create tables from Parquet files
def write_datasets (
  conn: duckdb.DuckDBPyConnection,
  parquetDict: dict
  ):
  
  try:
    # Create tables from Parquet files
    {
          table_name: duckdb.sql(f"""
           CREATE OR REPLACE TABLE {table_name} AS
           SELECT 
             * EXCLUDE (id, quarter, weekday),
             id::id_actigraphy_enum AS id,
             quarter::TEXT::quarter_enum AS quarter,
             weekday::TEXT::weekday_enum AS weekday
           FROM read_parquet(
             '{file_path}',
             hive_partitioning = true
             );""", connection=conn)
           for table_name, file_path in parquetDict.items()
           }
    #{name: conn.register(
    #  name, dataSet) for name, dataSet in parquetDict.items()}
    for key, value in parquetDict.items():
      result = conn.execute(f"SELECT COUNT(*) FROM {key}").fetchone()
      print(
           Style.BRIGHT + f"Successfully created table:"
           , Style.NORMAL + Fore.LIGHTBLUE_EX + f"{key},"
           , Style.BRIGHT + Fore.RESET + f"Row count:"
           , Style.NORMAL + Fore.LIGHTGREEN_EX + f"{result[0]}"
           , Fore.RESET
        )
  except Exception as e:
    print(f"Error writing dataset: {str(e)}")
    raise



