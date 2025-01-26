
from typing import Optional
import time
from auto_data_table.meta_operations import MetaDataStore
from auto_data_table import file_operations
from auto_data_table.prompt_execution import prompt_parser
from auto_data_table.prompt_execution.parse_code import execute_code_from_prompt, execute_gen_table_from_prompt
from auto_data_table.prompt_execution.parse_llm import execute_llm_from_prompt
from auto_data_table.database_lock import DatabaseLock
import pandas as pd
import random
import string

def _update_table_columns(to_change_columns: list, all_columns:list, instance_id: str, table_name: str, db_dir: str) -> list[str]:
    df = file_operations.get_table(instance_id, table_name, db_dir)
    columns = list(dict.fromkeys(df.columns).keys()) + [col for col in all_columns if col not in df.columns]
    for col in columns:
        if col not in all_columns:
            df.drop(col, axis=1)
        elif len(df) == 0:
            df[col] = []
        elif col in to_change_columns or col not in df.columns:
            df[col] = pd.NA
    file_operations.write_table(df, instance_id, table_name, db_dir) 

def _fetch_table_cache(external_dependencies:list, db_metadata:MetaDataStore, instance_id:str, table_name:str, db_dir:str,
                       start_time:float) -> prompt_parser.Cache:
    cache = {}
    cache['self'] = file_operations.get_table(instance_id, table_name, db_dir)

    for dep in external_dependencies:
        table, _, _,instance, latest = dep
        if latest:
            #instance = db_metadata.get_last_table_update(table, before_time=start_time)
            cache[table] = file_operations.get_table(instance, table, db_dir)
        else:
            cache[(table, instance)] = file_operations.get_table(instance, table, db_dir)
    return cache
    
def execute_table(table_name: str, db_dir: str, author: str, instance_id: str = 'TEMP'):
    instance_lock = DatabaseLock(db_dir, table_name, instance_id)
    instance_lock.acquire_exclusive_lock()
    prompts = file_operations.get_prompts(instance_id, table_name, db_dir)
    #print(prompts)
    #raise ValueError()
    if 'origin' in prompts['description']:
        origin = prompts['description']
    else:
        origin = None
    db_metadata = MetaDataStore(db_dir)
    start_time = time.time()
    top_pnames, to_change_columns, all_columns, internal_prompt_deps, external_deps = prompt_parser.parse_prompts(prompts, db_metadata , start_time,  table_name, db_dir)
    # print(top_pnames)
    # print(to_change_columns)
    # print(all_columns)
    # raise ValueError()
    # execute prompts
    dep_locks = []
    for pname in external_deps:
        for table, _, instance, _,_ in external_deps[pname]:
            lock = DatabaseLock(db_dir, table_name=table, instance_id=instance)
            lock.acquire_shared_lock()
            dep_locks.append(lock)

    data = {'origin': origin,
            'top_pnames': top_pnames, 'to_change_columns': to_change_columns, 'start_time': start_time,
            'all_columns': all_columns, 'internal_prompt_deps': internal_prompt_deps, 'external_deps': external_deps,
            'gen_columns': prompts[top_pnames[0]]['parsed_changed_columns']}
    process_id = db_metadata.start_new_process(author, 'execute_table', table_name, instance_id, start_time, data = data)
   # raise ValueError()
    _update_table_columns(to_change_columns,all_columns, instance_id, table_name, db_dir) 
    db_metadata.update_process_step(process_id, 'clear_table')
    for i, pname in enumerate(top_pnames):
        prompt = prompt_parser.convert_reference(prompts[pname])
        cache = _fetch_table_cache(external_deps[pname], db_metadata, instance_id, table_name, db_dir, start_time)
        if i == 0:
            execute_gen_table_from_prompt(prompt, cache, instance_id, table_name, db_dir) 
        else:
            if prompt['type'] == 'code':
                execute_code_from_prompt(prompt, cache, instance_id, table_name, db_dir)
            elif prompt['type'] == 'llm':
                execute_llm_from_prompt(prompt, cache, instance_id, table_name, db_dir)
        db_metadata.update_process_step(process_id, pname)
        #raise ValueError()
    rand_str = ''.join(random.choices(string.ascii_letters, k=5))
    perm_instance_id = str(int(time.time())) + rand_str
    db_metadata.update_process_data(process_id, {'perm_instance_id': perm_instance_id})
    file_operations.materialize_table(perm_instance_id, instance_id, table_name, db_dir)
    db_metadata.write_to_log(process_id)
    instance_lock.release_exclusive_lock()
    for lock in dep_locks:
        lock.release_shared_lock()


def restart_execute_table(author:str, process_id:str, db_dir:str): #TODO also allow clearing??
    db_metadata = MetaDataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try: 
        table_name = process.table_name
        top_pnames = process.data['top_pnames']
        to_change_columns = process.data['to_change_columns']
        all_columns = process.data['all_columns']
        internal_prompt_deps = process.data['internal_prompt_deps']
        external_deps = process.data['external_deps']
        instance_id = process.instance_id
        start_time = process.data['start_time']
        origin = process.data['origin']
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f'Error Fetching Data for process {process_id}. Not executed.')
        raise e
    
    if 'stop_execute' in process.complete_steps:
        file_operations.clear_table_instance(instance_id, table_name, db_dir)
        db_metadata.write_to_log(process_id, success=False)
        return

    #instance_lock = DatabaseLock(table_name, db_dir, instance_id)
    #instance_lock.acquire_exclusive_lock()
    prompts = file_operations.get_prompts(instance_id, table_name, db_dir)
    # dep_locks = []
    # for table, _, instance, _,_ in external_deps:
    #     lock = DatabaseLock(db_dir, table_name=table, instance_id=instance)
    #     lock.acquire_shared_lock()
    #     dep_locks.append(lock)

    if not 'clear_table' in process.complete_steps:
        _update_table_columns(to_change_columns,all_columns, instance_id, table_name, db_dir) 
        db_metadata.update_process_step(process_id, 'clear_table')
    
    for i, pname in enumerate(top_pnames):
        if pname in process.complete_steps:
            continue
        prompt = prompt_parser.convert_reference(prompts[pname])
        cache = _fetch_table_cache(external_deps[pname], db_metadata, instance_id, table_name, db_dir, start_time)
        if i == 0:
            execute_gen_table_from_prompt(prompt, cache, instance_id, table_name, db_dir, start_time) 
        else:
            if prompt['type'] == 'code':
                execute_code_from_prompt(prompt, cache, instance_id, table_name, db_dir)
            elif prompt['type'] == 'llm':
                execute_llm_from_prompt(prompt, cache, instance_id, table_name, db_dir)
        db_metadata.update_process_step(process_id, pname)
    
    if 'instance_id' in process.data:
        perm_instance_id = process.data['perm_instance_id']
    else:
        rand_str = ''.join(random.choices(string.ascii_letters, k=5))
        perm_instance_id = str(int(time.time())) + rand_str
        db_metadata.update_process_data(process_id, {'instance_id': perm_instance_id})
    file_operations.materialize_table(perm_instance_id, instance_id, table_name, db_dir)
    db_metadata.write_to_log(process_id)
    # instance_lock.release_exclusive_lock()
    # for lock in dep_locks:
    #     lock.release_shared_lock()

def delete_table(table_name: str, db_dir: str, author: str):
    db_metadata = MetaDataStore(db_dir)
    lock = DatabaseLock(db_dir, table_name)
    lock.acquire_exclusive_lock()
    operation = 'delete_table_instance'
    process_id = db_metadata.start_new_process(author, operation, table_name)
    file_operations.delete_table(table_name, db_dir)
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock()

def restart_delete_table(author:str, process_id:str, db_dir:str):
    db_metadata = MetaDataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f'Error Fetching Data for process {process_id}. Not executed.')
        raise e
    #lock = DatabaseLock(db_dir, table_name)
    #lock.acquire_exclusive_lock()
    file_operations.delete_table(table_name, db_dir)
    db_metadata.write_to_log(process_id)
    #lock.release_exclusive_lock()

def delete_table_instance(instance_id: str, table_name: str, db_dir: str, author: str):
    db_metadata = MetaDataStore(db_dir)
    operation = 'delete_table_instance'
    lock = DatabaseLock(db_dir, table_name, instance_id)
    lock.acquire_exclusive_lock()
    process_id = db_metadata.start_new_process(author, operation, table_name, instance_id)
    file_operations.delete_table(table_name, db_dir, instance_id)
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock()

def restart_delete_table_instance(author:str, process_id:str, db_dir:str):
    db_metadata = MetaDataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
        instance_id = process.instance_id
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f'Error Fetching Data for process {process_id}. Not executed.')
        raise e
    #lock = DatabaseLock(db_dir, table_name, instance_id)
    #lock.acquire_exclusive_lock()
    file_operations.delete_table(table_name, db_dir, instance_id)
    db_metadata.write_to_log(process_id)
    #lock.release_exclusive_lock()

def setup_table_instance(instance_id: str, table_name: str, db_dir: str, author: str, 
                         prev_name_id: str = '',
                         prompts: list[str] = [],
                         gen_prompt: str = ''):
    if len(prompts) != 0 and gen_prompt not in prompts:
        raise ValueError('Need to Define gen_prompt')
    db_metadata = MetaDataStore(db_dir)
    allow_multiple = db_metadata.get_table_multiple(table_name)
    if not allow_multiple and instance_id != 'TEMP':
        raise ValueError('Cannot Define Instance ID for Table without Versioning')
    elif allow_multiple and instance_id == 'TEMP':
        instance_id = 'TEMP_' + ''.join(random.choices(string.ascii_letters, k=10))
    lock = DatabaseLock(db_dir, table_name, instance_id)
    lock.acquire_exclusive_lock()
    if prev_name_id != '':
        prev_start_time = db_metadata.get_table_version_update(prev_name_id, table_name)
    else:
        prev_start_time = 0
    data = {'gen_prompt': gen_prompt, 'prompts': prompts, 'prev_name_id': prev_name_id}
    process_id = db_metadata.start_new_process(author, 'setup_table_instance', table_name, instance_id, data= data)
    file_operations.setup_table_instance(instance_id, table_name, db_dir, prev_name_id, prev_start_time, prompts, gen_prompt) 
    db_metadata.write_to_log(process_id)
    lock.release_exclusive_lock()

def restart_setup_table_instance(author:str, process_id:str, db_dir:str):
    db_metadata = MetaDataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
        instance_id = process.instance_id
        gen_prompt = process.data['gen_prompt']
        prompts = process.data['prompts']
        prev_name_id = process.data['prev_name_id']
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f'Error Fetching Data for process {process_id}. Not executed.')
    #lock = DatabaseLock(db_dir, table_name, instance_id)
    #lock.acquire_exclusive_lock()
    file_operations.setup_table_instance(instance_id, table_name, db_dir, prev_name_id, prompts, gen_prompt) 
    db_metadata.write_to_log(process_id)
    #lock.release_exclusive_lock()

def setup_table(table_name: str, db_dir: str, author: str, allow_multiple: bool = True):
    db_metadata = MetaDataStore(db_dir)
    lock = DatabaseLock(db_dir, table_name)
    lock.acquire_exclusive_lock()
    process_id  = db_metadata.start_new_process(author, 'setup_table', table_name, data= {'allow_multiple': allow_multiple})
    file_operations.setup_table_folder(table_name, db_dir)
    db_metadata.write_to_log(process_id)
    # write to metadata about multiple
    lock.release_exclusive_lock()

def restart_setup_table(author:str, process_id:str, db_dir:str):
    db_metadata = MetaDataStore(db_dir)
    process = db_metadata.update_process_restart(author, process_id)
    try:
        table_name = process.table_name
        allow_multiple = process.data['allow_multiple']
    except Exception as e:
        print(process)
        db_metadata.write_to_log(process_id, success=False)
        print(f'Error Fetching Data for process {process_id}. Not executed.')
    #lock = DatabaseLock(db_dir, table_name)
    #lock.acquire_exclusive_lock()
    file_operations.setup_table_folder(table_name, db_dir)
    db_metadata.write_to_log(process_id)
    #lock.release_exclusive_lock()




def restart_database(author:str, db_dir: str, excluded_processes: list[str] = []):
    db_lock = DatabaseLock(db_dir)
    db_lock.acquire_shared_lock()
    restart_lock = DatabaseLock(db_dir, table_name='RESTART')
    restart_lock.acquire_exclusive_lock()
    db_metadata = MetaDataStore(db_dir)
    db_metadata.teminate_previous_restarts()
    
    data = {'excluded_processes': excluded_processes, 'active_ids': active_ids}
    process_id = db_metadata.start_new_process(author, 'restart_database', table_name = '', data= data)
    db_metadata.write_to_log_after_restart()
    active_ids = db_metadata.get_process_ids()
    
    for id, operation in active_ids:
        if id == process_id:
            continue
        if id in excluded_processes:
            if operation == 'execute_table':
                db_metadata.update_process_step(id, 'stop_execute')
            else:
                raise ValueError(f"Can Only Stop Table Executions Right Now: {id}")
        if operation == 'setup_table':
            restart_setup_table(author, id, db_dir)
        elif operation == 'setup_table_instance':
            restart_setup_table_instance(author, id, db_dir)
        elif operation == 'delete_table':
            restart_delete_table(author, id, db_dir)
        elif operation == 'delete_table_instance':
            restart_delete_table_instance(author, id, db_dir)
        elif operation == 'execute_table':
            restart_execute_table(author, id, db_dir)
        db_metadata.update_process_step(process_id, (id, operation))
    
    db_metadata.write_to_log(process_id)
    db_lock.release_shared_lock()
    restart_lock.release_exclusive_lock()