import os
from typing import Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor
import pandas as pd

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



def _execute_code_from_prompt(index: Optional[int], prompt:dict, funct:Callable,  cache: dict[str, pd.DataFrame],
                              table_name:str) -> Any:
    df = cache[table_name]
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
        for table_name, table in prompt['table_arguments'].items():
            table_args[table_name] = cache[table]
    args = args | table_args
    results = funct(**args)
    return tuple(results)

def _execute_single_code_from_prompt(prompt:dict, funct:Callable, cache: dict) -> None:
    args = prompt_parser.get_table_value(prompt['arguments'], None, cache)
    table_args = {} 
    if 'table_arguments' in prompt:
        #(prompt['table_arguments'])
        #raise ValueError()
        for tname, table in prompt['table_arguments'].items():
            table_args[tname] = cache[table]
    args = args | table_args

    results = funct(**args)
    return results

def execute_code_from_prompt(prompt:Any, cache: dict[str, pd.DataFrame], 
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
    df = cache[table_name]
    if is_udf:
        indices = list(range(len(cache[table_name])))
        with ThreadPoolExecutor(max_workers=n_threads) as executor: 
            results = list(
                executor.map(
                    lambda i: _execute_code_from_prompt(i, prompt, funct, cache, table_name),
                    indices
                )
            )
            #print(results)
            #for i in indices:
            for col, values in zip(prompt['changed_columns'], zip(*results)):
                df[col] = values
    else:
        results = _execute_single_code_from_prompt(prompt, funct, cache)
        for col, values in prompt['changed_columns']:
            df[col] = results[col]
    file_operations.write_table(df, table_name, db_dir)


def execute_gen_table_from_prompt(prompt:Any, cache: dict[str, pd.DataFrame], table_name:str, db_dir: str) -> None:
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
    results = _execute_single_code_from_prompt(prompt, funct, cache)
    columns = list(df.columns)
    df = pd.merge(results, cache[table_name], how='left', on=prompt['changed_columns'])
    df = df[columns]
    file_operations.write_table(df, table_name, db_dir)
