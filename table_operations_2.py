
from typing import Optional
import time
from auto_data_table.meta_operations import MetaDataStore
from auto_data_table import file_operations
from auto_data_table.prompt_execution import prompt_parser
from auto_data_table.prompt_execution.parse_code import execute_code_from_prompt, execute_gen_table_from_prompt
from auto_data_table.prompt_execution.parse_llm import execute_llm_from_prompt

import pandas as pd
import random
import string

# TODO: change caching


def _update_table_columns(to_change_columns: list, all_columns:list, instance_id: str, table_name: str, db_dir: str) -> list[str]:
    df = file_operations.get_table(instance_id, table_name, db_dir)
    columns = set(df.columns).union(to_change_columns)
    for col in columns:
        if  col not in all_columns:
            df.drop(col, axis=1)
        elif len(df) == 0:
            df[col] = []
        elif col not in df.columns:
            df[col] = pd.NA
    file_operations.write_table(df, instance_id, table_name, db_dir) 

def _fetch_table_cache(external_dependencies, instance_id:str, table_name:str, db_dir:str):
    cache = {}
    cache['self'] = file_operations.get_table(instance_id, table_name, db_dir)

    for dep in external_dependencies:
        table, _, instance, _ = dep
        if instance == None:
            cache[table] = file_operations.get_table(instance, table, db_dir)
        else:
            cache[(table, instance)] = file_operations.get_table(instance, table, db_dir)
    return cache
    
def execute_table(table_name: str, db_dir: str, author: str, instance_id: str = 'TEMP'):
    prompts = file_operations.get_prompts(instance_id, table_name, db_dir)
    db_metadata = MetaDataStore(db_dir)
    process_id, start_time = db_metadata.start_new_process(operation = 'execute_table', table_name = table_name)
    top_pnames, to_change_columns, all_columns, internal_prompt_deps, external_deps = prompt_parser.parse_prompts(prompts, db_metadata , start_time,  table_name, db_dir)
    # execute prompts
    _update_table_columns(to_change_columns,all_columns, instance_id, table_name, db_dir) 
    for i, pname in enumerate(top_pnames):
        prompt = prompt_parser.convert_reference(prompts[pname])
        cache = _fetch_table_cache(external_deps[pname], instance_id, table_name, db_dir)
        if i == 0:
            execute_gen_table_from_prompt(prompt, cache, instance_id, table_name, db_dir, start_time) 
        else:
            if prompt['type'] == 'code':
                execute_code_from_prompt(prompt, cache, instance_id, table_name, db_dir)
            elif prompt['type'] == 'llm':
                execute_llm_from_prompt(prompt, cache, instance_id, table_name, db_dir)
    
    rand_str = ''.join(random.choices(string.ascii_letters, k=5))
    perm_instance_id = str(int(time.time())) + rand_str
    file_operations.materialize_table(perm_instance_id, instance_id, table_name, db_dir)
    
