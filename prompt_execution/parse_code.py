import os
from typing import Optional, Any, Callable
import threading
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

from auto_data_table import file_operations
from auto_data_table.prompt_execution import parse_helper


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



def _execute_code_from_prompt(index: Optional[int], prompt:dict, funct:Callable,  cache: dict[str, pd.DataFrame],
                              table_name:str, db_dir:str) -> Any:
    df = cache[table_name]
    empty = False
    current_values = []
    for col in prompt['changed_columns']:
        val = df.at[index, col]
        if df.at[index, col] != pd.NA:
            empty = True
            break
        else:
            current_values.append(val)
    if not empty:
        return tuple(current_values)
    args = parse_helper.parse_obj_from_prompt(prompt['arguments'], index, cache, table_name, db_dir)
    table_args = {} 
    if 'table_arguments' in prompt:
        for table_name, table in prompt['table_arguments'].items():
            table_args[table_name] = cache[table]
    
    args = args | table_args
    results = funct(**args)
    return tuple(results)

def _execute_single_code_from_prompt(prompt:dict, funct:Callable, cache: dict, table_name:str, db_dir:str) -> None:

    args = parse_helper.parse_obj_from_prompt(prompt['arguments'], None, cache, table_name, db_dir)
    table_args = {} 
    if 'table_arguments' in prompt:
        for table_name, table in prompt['table_arguments'].items():
            table_args[table_name] = cache[table]
    args = args | table_args

    results = funct(**args)
    return results

def execute_code_from_prompt(prompt:Any, cache: dict[str, pd.DataFrame], n_threads: int, 
                             table_name:str, db_dir:str, time_id:Optional[int]) -> None:
    is_udf = prompt['is_udf']
    is_global = prompt['is_global']
    code_file = prompt['code_file']
    prompt_function = prompt['function']

    if is_global:
        code_file = os.path.join('./code_functions/', code_file)
    else:
        code_file = os.path.join(db_dir, 'code_functions')
        code_file = os.path.join(code_file, code_file)
    
    funct, namespace = load_function_from_file(code_file, prompt_function)
    df = cache[table_name]
    if is_udf:
        indices = list(range(len(cache[table_name])))
        with ThreadPoolExecutor(max_workers=n_threads) as executor: 
            results = list(
                executor.map(
                    lambda i: _execute_code_from_prompt(prompt, funct, i, cache, table_name, db_dir),
                    indices
                )
            )
            for col, values in zip(prompt['changed_columns'], zip(*results)):
                df[col] = values
        n_rows = 0
    else:
        results = _execute_single_code_from_prompt(prompt, funct, cache, table_name, db_dir)
        if 'table_creation' in prompt:
            n_rows = len(results)
            columns = list(df.columns)
            df = pd.merge(results, cache[table_name], how='left', on=prompt['changed_columns'])
            df = df[columns]
        else:
            for col, values in prompt['changed_columns']:
                df[col] = results[col]
    file_operations.write_table(df, table_name, db_dir, time_id)
    return n_rows
