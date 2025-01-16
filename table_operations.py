import time
import pandas as pd
from typing import Optional

from  auto_data_table import file_operations
from auto_data_table.meta_operations import MetaDataStore, TempLog
from auto_data_table.prompt_execution import prompt_execution
from auto_data_table.prompt_execution import prompt_parser 
from auto_data_table.database_lock import DatabaseLock
# does delete work for temp instances??

# write to temp files
def _update_table_columns(columns: list, table_name: str, db_dir: str, replace: bool = True) -> list[str]:
    df = file_operations.get_table(table_name, db_dir)
    for col in columns:
        if len(df) == 0:
            df[col] = []
        elif replace or col not in df.columns:
            df[col] = pd.NA
    file_operations.write_table(df, table_name, db_dir) 
    return list(df.columns)


def _get_restart_data(operation:str, logs:MetaDataStore, table_name:str, 
                      restart_time:float) -> tuple[dict[str, TempLog], float, list[float]]:
    temp_logs = logs.get_temp_logs()
    op_log = temp_logs[table_name][operation]
    start_time = op_log.start_time
    if 'restarts' in op_log.data:
        restarts = op_log.data['restarts']
        
    else:
        restarts = []
    restarts.append(restart_time)
    #logs.add_restart_time(operation, restart_time, table_name)

    return temp_logs[table_name], start_time, restarts

def setup_table(table_name: str, db_dir: str, author: str, replace: bool = False, allow_multiple: bool = True):
    lock = DatabaseLock(table_name, db_dir)
    lock.acquire_exclusive_lock()
    start_time = int(time.time())
    logs = MetaDataStore(db_dir)
    logs.write_to_temp_log(operation = 'start_setup_table', table_name = table_name,
                           author = author, start_time = start_time, data = {'replace': replace, 'allow_multiple': allow_multiple}
                           ) # TODO: Start Operation
    if not replace and table_name in logs.get_all_tables():
        replace = False
    else:
        replace = True
    logs.write_to_temp_log(operation = 'check_setup_table', table_name = table_name,
                           author = author, start_time = start_time, 
                           data = {'replace': replace}
                           )
    if not replace:
        logs.write_to_setup_table_log(table_name = table_name, author = author, 
                                  start_time = start_time, executed = False, allow_multiple = None)
    else:
        file_operations.setup_table_folder(table_name, db_dir, allow_multi=allow_multiple)
        logs.write_to_setup_table_log(table_name = table_name, author = author, 
                                    start_time = start_time, executed = True, allow_multiple= allow_multiple)
    lock.release_exclusive_lock()
    return replace

def restart_setup_table(table_name: str, db_dir: str, author: str):
    #TODO: add locks???
    restart_time = int(time.time())
    logs = MetaDataStore(db_dir)
    # get all the restarts
    temp_logs, start_time, restarts = _get_restart_data('start_setup_table', logs, table_name, restart_time)
    if not 'check_setup_table' in temp_logs:
        replace = temp_logs['check_setup_table' ].data['replace']
        if not replace and table_name in logs.get_all_tables():
            replace_ = False
        else:
            replace_ = True 
        logs.write_to_temp_log(operation = 'check_setup_table', table_name = table_name,
                            author = author, start_time = start_time, 
                            data = {'replace': replace_}
                            )
    else:
        replace_ = temp_logs['check_setup_table' ].data['replace']
    if replace_:
        file_operations.setup_table_folder(table_name, db_dir)
    logs.write_to_setup_table_log(table_name = table_name, author = author, 
                                  start_time = start_time, restarts = restarts, executed = replace)
    return replace

def setup_table_instance(table_id: str, table_name: str, db_dir: str, author: str, 
                         prev_name_id: Optional[str] = None,
                         prompts: list[str] = [],
                         gen_prompt: str = ''):
    
    if len(prompts) != 0 and gen_prompt not in prompts:
        raise ValueError('Need to Define gen_prompt')
    
    if prev_name_id != None:
        prev_lock = TableLock(table_name, db_dir, prev_name_id)
        prev_lock.acquire_read_lock()
    lock = TableLock(table_name, db_dir, table_id)
    lock.acquire_write_lock()
    start_time = int(time.time())
    logs = MetaDataStore(db_dir)
    logs.write_to_temp_log(operation = 'start_setup_table_instance', table_name = table_name,
                           author = author, start_time = start_time, 
                           data = {'prev_name_id': prev_name_id, 
                                   'prompts': prompts, 'gen_prompt':gen_prompt}
                           )
    file_operations.setup_temp_table(table_name, db_dir, prev_name_id, prompts, gen_prompt) 
    logs.write_to_setup_instance_log(table_name = table_name, prev_name_id = prev_name_id,
                                   author = author, 
                                  start_time = start_time, prompts=prompts)
    lock.release_write_lock()
    if prev_name_id != None:
        prev_lock.release_read_lock()

def restart_setup_table_instance(table_name: str, db_dir: str, author: str):
    restart_time = int(time.time())
    logs = MetaDataStore(db_dir)
    temp_logs, start_time, restarts  = _get_restart_data('start_setup_table_instance', logs, table_name, restart_time)
    prev_name_id = temp_logs['start_setup_table_instance'].data['prev_name_id']
    prompts = temp_logs['start_setup_table_instance'].data['prompts']
    gen_prompt = temp_logs['start_setup_table_instance'].data['gen_prompt']
    file_operations.setup_temp_table(table_name, db_dir, prev_name_id, prompts, gen_prompt) 

    logs.write_to_setup_instance_log(table_name = table_name, prev_name_id = prev_name_id,
                                   author = author, 
                                  start_time = start_time, prompts=prompts, restarts=restarts)
    

def delete_table(table_name: str, db_dir: str, author: str, time_id: Optional[int]):
    lock = file_operations.lock_database(db_dir)
    start_time = int(time.time())
    logs = MetaDataStore(db_dir)
    logs.write_to_temp_log(operation = 'start_delete_table', table_name = table_name,
                           author = author, start_time = start_time,
                           data={'time_id': time_id} )
    file_operations.delete_table(table_name, db_dir, time_id)
    logs.write_to_delete_table_log(table_name = table_name, time_id = time_id,
                                   author = author, start_time = start_time, op_time = int(time.time()))
    file_operations.unlock_database(lock)
        
def restart_delete_table(table_name: str, db_dir: str, author: str):
    restart_time = int(time.time()) 
    logs = MetaDataStore(db_dir)
    temp_logs, start_time, restarts  = _get_restart_data('start_delete_table', logs, table_name, restart_time)
    time_id = temp_logs['start_delete_table'].data['time_id']
    file_operations.delete_table(table_name, db_dir, time_id)
    logs.write_to_delete_table_log(table_name = table_name, time_id = time_id,
                                   author = author, start_time = start_time, restarts = restarts)


def execute_table(table_name: str, db_dir: str, author: str): # ADD LOGGING
    lock = file_operations.lock_table(table_name, db_dir)
    start_time = time.time()
    logs = MetaDataStore(db_dir)
    logs.write_to_temp_log(operation = 'start_execute_table', table_name = table_name,
                           author = author, start_time = start_time)
    
    prompts = file_operations.get_prompts(table_name, db_dir)
    metadata = prompts['metadata']
    del prompts['metadata']
    for name, prompt in prompts.items():
        
        if 'parsed_changed_columns' not in prompt:
            prompt['parsed_changed_columns'] = prompt_parser.get_changed_columns(prompt) # need to deal with?
            file_operations.write_prompt(name, prompt, table_name, db_dir)
    table_generator = prompts[metadata['table_generator']]
    del prompts[metadata['table_generator']]
    prompt_names = prompt_parser.get_execution_order(prompts, table_name)
    if 'origin' in metadata:
        changed_columns = prompt_parser.get_replacement_columns(prompt_names, prompts, metadata['origin'], start_time,
                                                                table_generator['parsed_changed_columns'] , logs, table_name, db_dir, start_time) 
    else:
        changed_columns = prompt_parser.get_all_columns(prompts)
    changed_columns += table_generator['parsed_changed_columns']
    all_columns = _update_table_columns(changed_columns, table_name, db_dir) 
    data = {'all_columns':all_columns, 'changed_columns': changed_columns, 
            'execution_order':prompt_names, 'table_generator_name': metadata['table_generator']}
    logs.write_to_temp_log(operation = 'update_columms', table_name = table_name,
                           author = author, start_time = start_time, 
                           data = data)
    table_generator = prompt_parser.convert_reference(table_generator, table_name)
    prompt_execution.execute_generation(table_generator, table_name, db_dir, start_time) 
    
    logs.write_to_temp_log(operation = 'table_generator', table_name = table_name,
                           author = author, start_time = start_time)

    for name in prompt_names:
        prompt = prompt_parser.convert_reference(prompts[name], table_name)
        prompt_execution.execute_prompt(prompt, table_name, db_dir, start_time)
        logs.write_to_temp_log(operation = '{name}', table_name = table_name,
                           author = author, start_time = start_time) 
    
    time_id = file_operations.materialize_table(table_name, db_dir)
    logs.write_to_execute_table_log(changed_columns = changed_columns, all_columns = all_columns, 
                            time_id = time_id, table_name = table_name,
                           author = author, start_time = start_time)
    lock.release()
    # if copy:
    #     setup_table_instance(table_name, db_dir, 'execute_table', time_id)

def restart_execute_table(table_name: str, db_dir: str, author: str) -> bool: # ADD LOGGING
    restart_time = time.time()
    logs = MetaDataStore(db_dir)
    temp_logs, start_time, restarts  = _get_restart_data('start_execute_table', logs, table_name, restart_time)

    prompts = file_operations.get_prompts(table_name, db_dir)
    metadata = prompts['metadata']
    del prompts['metadata']
    if 'update_columms' in temp_logs:
        all_columns = temp_logs['update_columms'].data['all_columns']
        changed_columns = temp_logs['update_columms'].data['changed_columns']
        prompt_names = temp_logs['update_columms'].data['execution_order']
        table_generator = prompts[metadata['table_generator']]
        del prompts[metadata['table_generator']]
    else:
        for name, prompt in prompts.items():
            if 'parsed_changed_columns' not in prompt:
                prompt['parsed_changed_columns'] = prompt_parser.get_changed_columns(prompt) # need to deal with?
                file_operations.write_prompt(name, prompt, table_name, db_dir)
    
        table_generator = prompts[metadata['table_generator']]
        del prompts[metadata['table_generator']]
        prompt_names = prompt_parser.get_execution_order(prompts)
    
        if 'origin' in metadata:
            changed_columns = prompt_parser.get_replacement_columns(prompts, start_time, metadata['origin'], logs, table_name, db_dir) 
        else:
            changed_columns = prompt_parser.get_all_columns(prompts) 
    
        all_columns = _update_table_columns(changed_columns, table_name, db_dir)
        data = {'all_columns':all_columns, 'changed_columns': changed_columns, 
            'execution_order':prompt_names, 'table_generator_name': metadata['table_generator']}
        logs.write_to_temp_log(operation = 'update_columms', table_name = table_name,
                            author = author, start_time = start_time, 
                            data = data) 
    if 'table_generator' not in temp_logs:
        table_generator = prompt_parser.convert_reference(table_generator)
        prompt_execution.execute_generation(table_generator, start_time, table_name, db_dir) 
        logs.write_to_temp_log(operation = 'table_generator', table_name = table_name,
                           author = author, start_time = start_time)
    for name in prompt_names:
        if name in temp_logs:
            continue
        prompt = prompt_parser.convert_reference(prompts[name])
        prompt_execution.execute_prompt(prompt, table_name, start_time, db_dir)
        logs.write_to_temp_log(operation = '{name}', table_name = table_name,
                           author = author, start_time = start_time) 
    
    time_id = file_operations.materialize_table(table_name, db_dir)
    logs.write_to_execute_table_log(changed_columns = changed_columns, all_columns = all_columns,
                                     time_id = time_id, restarts = restarts,
                                     table_name = table_name, author = author, start_time = start_time)    

def clean_up_after_restart(db_dir: str, author: str) -> None:
    lock = file_operations.lock_database(db_dir)
    start_time = time.time()
    logs = MetaDataStore(db_dir)
    temp_logs = logs.get_temp_logs()
    in_progress_tables = []
    for table_name in temp_logs:
        if len(temp_logs[table_name]) > 0:
            in_progress_tables.append(table_name)
    tables = logs.get_all_tables()
    for table_name in tables:
        if len(temp_logs[table_name]) > 0:
            if 'write_final_log' in temp_logs[table_name]:
                logs.write_to_log(temp_logs[table_name]['write_final_log'])
            elif 'start_setup_table' in temp_logs[table_name]:   
                restart_setup_table(table_name, db_dir, author)
            elif 'start_setup_table_instance' in temp_logs[table_name]:   
                restart_setup_table_instance(table_name, db_dir, author)
            elif 'start_delete_table' in temp_logs[table_name]:
                restart_delete_table(table_name, db_dir, author)
            elif 'start_execute_table' in temp_logs[table_name]:
                restart_execute_table(table_name, db_dir, author)
    logs.write_to_restart_db_log(author=author, start_time = start_time, table_name = 'DATABASE',
                                 in_progress_tables = in_progress_tables)
    file_operations.unlock_database(lock)