'''
General Parsing Function

'''
from typing import Optional, Union, Any
from dataclasses import dataclass
from parse_code import execute_code_from_prompt
from parse_llm import execute_llm_from_prompt
from auto_data_table.meta_operations import MetaDataStore
import os
from parse_helper_aux import *
from dataclasses_json import dataclass_json

import pandas as pd

@dataclass_json
@dataclass
class TableReference:
    table: str
    column: str
    key: Optional[dict[str, Union["TableReference", str]]] = None
    

@dataclass
class TableString:
    text: str
    references: list[TableReference]

def get_executing_prompts(prompts:dict[str, Any], old_prompts:dict[str, Any], logs: MetaDataStore, 
                          table_name: str, db_dir:str, start_time: int) -> list[str]:
    last_update_time = logs.get_last_table_update(table_name)
    table_dir = os.path.join(db_dir, table_name)
    top_names = get_prompts_order(prompts, table_name)
    # figure out which prompts actually need to be changed
    to_execute_names = []
    to_change_columns = set()
    for name in top_names:
        to_change_columns[name]
        update = False
        if name not in old_prompts:
            update = True
        else:
            prompt_dir = os.path.join(table_dir, 'new_prompts')
            prompt_path = os.path.join(prompt_dir, f'{name}.yaml')
            old_prompt_dir = os.path.join(table_dir, 'prompts')
            old_prompt_path = os.path.join(old_prompt_dir, f'{name}.yaml')
            check = files_are_equal(prompt_path, old_prompt_path)
            # only consider if the prompt isn't changed
            if check:
                for dep in prompts[name]['dependencies']:
                    if '.' in dep:
                        dep_table = dep.split('.')[0]
                        dep_col = dep.split('.')[1]
                        if dep_table == table_name or dep_table == 'self':
                            if dep in to_change_columns:
                                update = True
                                break
                        elif logs.get_last_column_update(dep_table, dep_col, start_time) > last_update_time:
                            update = True
                            break
                    else:
                        if logs.get_last_table_update(dep, start_time) > last_update_time:
                            update = True
                            break
            if not check:
                update = True
        if update:
            to_execute_names.append(name)
            to_change_columns.update(prompts[name]["changed_columns"])     
    return to_execute_names
        

def get_prompts(table_name:str, db_dir: str, time_id: Optional[int] = None) -> dict[str, Any]:
    table_dir = os.path.join(db_dir, table_name)
    if time_id == None:
        temp_dir = os.path.join(table_dir, time_id)
    else:
        temp_dir = os.path.join(table_dir, 'TEMP')
    prompt_dir = os.path.join(temp_dir, 'prompts')
    prompts = {}
    for item in os.listdir(prompt_dir):
        if item.endswith('.yaml'):
            name = item.split('.')[0]
            prompt_path = os.path.join(prompt_dir, item)
            prompt = parse_prompt(prompt_path)
            prompts[name] = prompt
    return prompts

def parse_obj_from_prompt(prompt:Any, index:Optional[int], cache:dict[str, pd.DataFrame], 
                          table_name: str, db_dir:str) -> Any:
    if isinstance(prompt, TableString):
        prompt_ = prompt.text
        for ref in prompt.references: 
            ref_ = read_table_reference(ref, index=index, cache= cache, table_name= table_name, db_dir=db_dir)
            prompt_ = prompt_.replace('<<>>', ref_, 1)
    elif isinstance(prompt, TableReference):
        prompt_ = read_table_reference(prompt, index=index, cache= cache, table_name= table_name, db_dir=db_dir)
    elif isinstance(prompt, dict):
        prompt_ = {}
        for key in prompt:
            temp = read_table_reference(prompt[key], index=index, cache= cache, table_name= table_name, db_dir=db_dir)
            prompt_[key] = temp
    
    elif isinstance(prompt, list):
        prompt_ = []
        for val in prompt:
            temp = read_table_reference(val, index=index, cache= cache, table_name= table_name, db_dir=db_dir)
            prompt_.append(temp)
    else:
        prompt_ = prompt
    return prompt_

def get_changed_columns(prompt: Any)-> list[str]:
    if prompt['type'] == 'code':
        changed_columns =  prompt['changed_columns']
    elif prompt['type'] == 'llm':
        col = prompt['changed_columns'][0]
        changed_columns = []
        for i in range(len(prompt['questions']) - 1):
            changed_columns.append(col + str(i + 1))
        if prompt['output_type'] != 'freeform':
            changed_columns.append(col + str(i + 1))
        changed_columns.append(col)
    return changed_columns
    
def execute_prompt(prompt:Any, table_name:str, start_time:int, 
                   db_dir:str, time_id: Optional[int] = None) -> None:
    tables = get_dependent_tables(prompt['dependencies'], table_name)
    cache = fetch_table_cach(tables, start_time, db_dir)
    
    if 'n_threads' in prompt:
        n_threads = prompt['n_threads']
    else:
        n_threads = 1        

    if prompt['type'] == 'code':
        execute_code_from_prompt(prompt, cache,  n_threads, table_name, db_dir, time_id)
    elif prompt['type'] == 'llm':
        execute_llm_from_prompt(prompt, cache,  n_threads, table_name, db_dir, time_id)

