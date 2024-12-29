import os
from typing import Optional, Any, Callable
import threading
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from auto_data_table import file_operations
from auto_data_table.parse_prompts import parse_helper


def load_function_from_file(file_path:str, function_name:str) -> tuple[Callable, Any]:
    # Define a namespace to execute the file in
    namespace = {}
    # Read and execute the file
    with open(file_path, 'r') as file:
        exec(file.read(), namespace)
    # Retrieve the function from the namespace 
    if function_name in namespace:
        return namespace[function_name], namespace
    else:
        raise AttributeError(f"Function '{function_name}' not found in '{file_path}'")

lock = threading.Lock()

def _execute_code_from_prompt(prompt:Any, funct:Callable, index: Optional[int], 
                              table_name:str, db_dir:str) -> None:
    global cache
    df = cache[table_name]
    if prompt['is_udf'] and df.iloc[index, df.columns.get_loc(i)] != '':
        return
    args = parse_helper.parse_obj_from_prompt(prompt['arguments'], index, cache, table_name, db_dir)
    table_args = {} 
    if 'table_arguments' in prompt:
        for table_name, table in prompt['table_arguments'].items():
            table_args[table_name] = cache[table]
    
    args = args | table_args

    results = funct(**args)

    if prompt['is_udf']:
        with lock:
            for i, column in enumerate(prompt['changed_columns']):
                df.iloc[index, df.columns.get_loc(column)] = results[i]

    else:
        with lock:
            if len(df) == len(results):
                for i, column in enumerate(prompt['changed_columns']):
                    df[column] = results[column]
            else:
                df = results

def execute_code_from_prompt(prompt:Any, cache: dict[str, pd.DataFrame], n_threads: int, 
                             table_name:str, db_dir:str, time_id:Optional[int]) -> None:
    is_udf = prompt['is_udf']
    is_global = prompt['is_global']
    code_file = prompt = prompt['code_file']
    prompt_function = prompt = prompt['function']

    if is_global:
        code_file = os.path.join('../code_functions/', code_file)
    else:
        code_file = os.path.join(db_dir, 'code_functions')
        code_file = os.path.join(code_file, code_file)
    
    funct, namespace = load_function_from_file(code_file, prompt_function)
    indices = list(range(len(cache[table_name])))
    if is_udf:
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = [
                executor.submit(_execute_code_from_prompt, prompt, funct, i, table_name, db_dir) for i in indices
            ]
            for future in futures: 
                future.result()  # Wait for completion and raise exceptions if any
    else:
        _execute_code_from_prompt(prompt, funct,  None, table_name, db_dir)
    file_operations.write_table(cache[table_name], table_name, db_dir, time_id)
