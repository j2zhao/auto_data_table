

from typing import Optional, Any

from auto_data_table.prompt_execution.parse_code import execute_code_from_prompt, execute_gen_table_from_prompt
from auto_data_table.prompt_execution.parse_llm import execute_llm_from_prompt
from auto_data_table.meta_operations import MetaDataStore
from auto_data_table.file_operations import get_table
import pandas as pd

def execute_generation(prompt:Any, start_time:int, table_name:str, 
                   db_dir:str) -> None:
    tables = get_dependent_tables(prompt['dependencies'], table_name)
    if table_name not in table_name:
        tables.append(table_name)
    cache = fetch_table_cache(tables, start_time, table_name, db_dir)
    execute_gen_table_from_prompt(prompt, cache, table_name, db_dir)
    
    
def execute_prompt(prompt:Any, start_time:int, table_name:str, 
                   db_dir:str) -> None:
    tables = get_dependent_tables(prompt['dependencies'], table_name)
    if table_name not in table_name:
        tables.append(table_name)
    cache = fetch_table_cache(tables, start_time, table_name, db_dir)  

    if prompt['type'] == 'code':
        return execute_code_from_prompt(prompt, cache, table_name, db_dir)
    elif prompt['type'] == 'llm':
        return execute_llm_from_prompt(prompt, cache, table_name, db_dir)
    

def get_dependent_tables(dependencies:list[str], table_name:str) -> list[str]:
    tables = set()
    for dep in dependencies:
        if '.' in dep:
            dep_table = dep.split('.')[0]
            if dep_table == 'self':
                tables.add(table_name)
            else:
                tables.add(dep_table)

        else:
            tables.add(dep)
    return list(tables)

def fetch_table_cache(table_names:list[str], start_time:int, table_name:str, db_dir:str)-> dict[str, pd.DataFrame]:
    cache = {}
    logs = MetaDataStore(db_dir)
    for name in table_names:
        if name != table_name:
            table_time = logs.get_last_table_update(name, start_time)
        else:
            table_time = None
        df = get_table(table_name, db_dir, table_time)
        cache[table_name] = df
    return cache