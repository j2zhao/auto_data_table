import os
from typing import Optional, Any, Callable, Union
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import re

from auto_data_table import file_operations
from auto_data_table.prompt_execution import prompt_parser 

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



def _execute_code_from_prompt(index: int, prompt:prompt_parser.Prompt, funct:Callable,  cache: prompt_parser.Cache) -> tuple[Any]:
    df = cache['self']
    empty = False
    current_values = []
    for col in prompt['changed_columns']:
        val = df.at[index, col]
        if pd.isna(df.at[index, col]):
            empty = True
            break
        else:
            current_values.append(val)
    if not empty:
        return tuple(current_values)
    
    args = prompt_parser.get_table_value(prompt['arguments'], index, cache)
    #print(args)
    table_args = {} 
    if 'table_arguments' in prompt:
        for tname, table in prompt['table_arguments'].items():
            match = re.match(r"^(\w+)(?:\((\w+)\))?$", table)
            table_name = match.group(1)
            instance_id = match.group(2)
            if instance_id != None:
                table_key = table_name
            else:
                table_key = (table_name,instance_id)
            table_args[tname] = cache[table_key]
    args = args | table_args
    results = funct(**args)
    return tuple(results)

def _execute_single_code_from_prompt(prompt:prompt_parser.Prompt, funct:Callable, 
                                     cache:  prompt_parser.Cache) -> Any:
    args = prompt_parser.get_table_value(prompt['arguments'], None, cache)
    table_args = {} 
    if 'table_arguments' in prompt:
        for tname, table in prompt['table_arguments'].items():
            match = re.match(r"^(\w+)(?:\((\w+)\))?$", table)
            table_name = match.group(1)
            instance_id = match.group(2)
            if instance_id != None:
                table_key = table_name
            else:
                table_key = (table_name,instance_id)
            table_args[tname] = cache[table_key]
    args = args | table_args

    results = funct(**args)
    return results

def execute_code_from_prompt(prompt:prompt_parser.Prompt, cache:  prompt_parser.Cache,
                             table_name:str, db_dir:str) -> None:
    is_udf = prompt['is_udf']
    is_global = prompt['is_global']
    code_file = prompt['code_file']
    prompt_function = prompt['function']
    if 'n_threads' in prompt:
        n_threads = prompt['n_threads']
    else:
        n_threads = 1   
    
    if is_global:
        code_file = os.path.join('./code_functions/', code_file)
    else:
        code_file = os.path.join(db_dir, 'code_functions')
        code_file = os.path.join(code_file, code_file)
    
    funct, namespace = load_function_from_file(code_file, prompt_function)
    df = cache['self']
    if is_udf:
        indices = list(range(len(df)))
        with ThreadPoolExecutor(max_workers=n_threads) as executor: 
            results = list(
                executor.map(
                    lambda i: _execute_code_from_prompt(i, prompt, funct, cache),
                    indices
                )
            )
            for col, values in zip(prompt['changed_columns'], zip(*results)):
                df[col] = values
    else:
        results = _execute_single_code_from_prompt(prompt, funct, cache)
        for col, values in prompt['changed_columns']:
            df[col] = results[col]
    file_operations.write_table(df, table_name, db_dir)


def execute_gen_table_from_prompt(prompt:prompt_parser.Prompt, cache: prompt_parser.Cache, 
                                  instance_id:str, table_name:str, db_dir: str) -> None:
    is_global = prompt['is_global']
    code_file = prompt['code_file']
    prompt_function = prompt['function']

    if is_global:
        code_file = os.path.join('./code_functions/', code_file)
    else:
        code_file = os.path.join(db_dir, 'code_functions')
        code_file = os.path.join(code_file, code_file)
    
    funct, namespace = load_function_from_file(code_file, prompt_function)
    results = _execute_single_code_from_prompt(prompt, funct, cache)
    columns = list(cache['self'].columns)
    df = pd.merge(results, cache['self'], how='left', on=prompt['changed_columns'])
    df = df[columns]
    file_operations.write_table(df, instance_id, table_name, db_dir)
