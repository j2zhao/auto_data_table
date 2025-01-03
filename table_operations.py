import time

from  auto_data_table import file_operations
from auto_data_table.meta_operations import MetaDataStore, TempLog, LOG_MAP
from auto_data_table.prompt_execution.parse_helper import *

import pandas as pd

# write to temp files
def _update_table_columns(columns: list, table_name: str, db_dir: str, replace: bool = True) -> list[str]:
    df = get_table(table_name, db_dir)
    for col in columns:
        if len(df) == 0:
            df[col] = []
        elif replace or col not in df.columns:
            df[col] = pd.NA
    file_operations.write_table(df, table_name, db_dir) 
    return list(df.columns)


def _get_restart_data(logs:MetaDataStore, table_name:str, 
                      restart_time:float) -> tuple[dict[str, list[TempLog]], float, list[float]]:
    temp_logs = logs.get_temp_logs()
    temp_logs_ = temp_logs[table_name]
    start_time = temp_logs_['start_setup_table'][0].start_time
    restarts = []
    if 'restart_setup_table' in temp_logs_:
        for log in temp_logs_['restart_setup_table']:
            restarts.append(log.op_time)
    restarts.append(restart_time)
    return temp_logs_, start_time, restarts

def write_to_log_from_temp(logs:MetaDataStore, table_name:str):
    temp_logs = logs.get_temp_logs()
    log_entry = temp_logs[table_name]['write_final_log'][0].data['log']
    logs.write_to_log(log_entry)

def setup_table(table_name: str, db_dir: str, author: str, replace: bool = False):
    start_time = int(time.time())
    lock = file_operations.lock_database(db_dir)
    logs = MetaDataStore(db_dir)
    logs.write_to_temp_log(operation = 'start_setup_table', table_name = table_name,
                           author = author, start_time = start_time
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
                                  start_time = start_time, executed = False)
    else:
        file_operations.setup_table_folder(table_name, db_dir)
        logs.write_to_setup_table_log(table_name = table_name, author = author, 
                                    start_time = start_time, executed = True)
    file_operations.unlock_database(lock)
    return replace

def restart_setup_table(table_name: str, db_dir: str, author: str):
    lock = file_operations.lock_database(db_dir)
    restart_time = int(time.time())
    logs = MetaDataStore(db_dir)
    # get all the restarts
    temp_logs, start_time, restarts = _get_restart_data(logs, table_name, restart_time)
    logs.write_to_temp_log(operation = 'restart_setup_table', table_name = table_name,
                           author = author, start_time = start_time
                           )
    if not 'check_setup_table' in temp_logs:
        replace = temp_logs['start_setup_table' ][0].data['replace']
        if not replace and table_name in logs.get_all_tables():
            replace = False
        else:
            replace = True   
    else:
        replace = temp_logs['check_setup_table' ][0].data['replace']
        check = file_operations.check_folder(table_name, db_dir)
        if replace and check:
            file_operations.delete_table(table_name, db_dir)
    logs.write_to_temp_log(operation = 'check_setup_table', table_name = table_name,
                           author = author, start_time = start_time, 
                           data = {'replace': replace}
                           )
    if replace:
        file_operations.setup_table_folder(table_name, db_dir)
    logs.write_to_setup_table_log(table_name = table_name, author = author, 
                                  start_time = start_time, restarts = restarts, executed = replace)
    file_operations.unlock_database(lock)
    return replace

def setup_table_instance(table_name: str, db_dir: str, author: str, prev_time_id: Optional[int] = None):
    lock = file_operations.lock_table('DATABASE', db_dir)
    lock2 = file_operations.lock_table(table_name, db_dir)
    start_time = int(time.time())
    logs = MetaDataStore(db_dir)
    logs.write_to_temp_log(operation = 'start_setup_table_instance', table_name = table_name,
                           author = author, start_time = start_time, 
                           data = {'prev_time_id': Optional}
                           )
    file_operations.setup_temp_table(table_name, db_dir, prev_time_id) 
    logs.write_to_setup_table_log(table_name = table_name, prev_time_id = prev_time_id,
                                   author = author, 
                                  start_time = start_time)
    lock.release()
    lock2.release()

def restart_setup_table_instance(table_name: str, db_dir: str, author: str):
    restart_time = int(time.time())
    lock = file_operations.lock_table('DATABASE', db_dir)
    lock2 = file_operations.lock_table(table_name, db_dir)
    logs = MetaDataStore(db_dir)
    temp_logs, start_time, restarts  = _get_restart_data(logs, table_name, restart_time)
    prev_time_id = temp_logs['start_setup_table_instance'][0].data['prev_time_id']
    logs.write_to_temp_log(operation = 'restart_setup_table_instance', table_name = table_name,
                           author = author, start_time = start_time)
    file_operations.setup_temp_table(table_name, db_dir, prev_time_id) 
    logs.write_to_setup_table_log(table_name = table_name, prev_time_id = prev_time_id,
                                   author = author, start_time = start_time, restarts = restarts)
    lock.release()
    lock2.release()


def delete_table(table_name: str, db_dir: str, author: str, time_id: Optional[int]):
    start_time = int(time.time()) # TODO: Start Operation
    lock = file_operations.lock_database(db_dir)
    logs = MetaDataStore(db_dir)
    logs.write_to_temp_log(operation = 'start_delete_table', table_name = table_name,
                           author = author, start_time = start_time,
                           data={'time_id': time_id} )
    file_operations.delete_table(table_name, db_dir, time_id)
    logs.write_to_delete_table_log(table_name = table_name, time_id = time_id,
                                   author = author, start_time = start_time, op_time = int(time.time()))
    file_operations.unlock_database(lock)
        
def restart_delete_table(table_name: str, db_dir: str, author: str):
    restart_time = int(time.time()) # TODO: Start Operation
    lock = file_operations.lock_database(db_dir)
    logs = MetaDataStore(db_dir)
    temp_logs, start_time, restarts  = _get_restart_data(logs, table_name, restart_time)
    time_id = temp_logs['start_delete_table'][0].data['time_id']
    logs.write_to_temp_log(operation = 'restart_delete_table', table_name = table_name,
                           author = author, start_time = start_time)
    file_operations.delete_table(table_name, db_dir, time_id)
    logs.write_to_delete_table_log(table_name = table_name, time_id = time_id,
                                   author = author, start_time = start_time, restarts = restarts)
    file_operations.unlock_database(lock)

# # update rows
def execute_table(table_name: str, db_dir: str, author: str) -> bool: # ADD LOGGING
    # TODO: ERROR Handling, fix prompt execution, 
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
    prompt_names = prompt_parser.get_execution_order(prompts)
    
    if 'origin' in metadata:
        update_columns = prompt_parser.get_replacement_columns(prompts, start_time, metadata['origin'], logs, table_name, db_dir) 
    else:
        update_columns = prompt_parser.get_all_columns(prompts) 
    
    all_columns = _update_table_columns(update_columns, table_name, db_dir) 
    #TODO: record after this? -> so we don't overwrite anything -> record execution order and executed_prompts, update_columns, and all_columns

    # TODO: record update_row before execution? okay
    table_generator = prompt_parser.convert_references(table_generator)
    n_update_rows = prompt_execution.execute_generation(table_generator, logs, table_name, db_dir, start_time) 
    # TODO: record after this so we know our update rows? -> we might still be wrong???? -> 
    
    for name in prompt_names:
        prompt = prompt_parser.convert_references(prompts[name])
        prompt_execution.execute_prompt(prompt, table_name, start_time, db_dir)
        #TODO: record after execution
    
    time_id = file_operations.materialize_table(table_name, db_dir)
    # TODO: record final file
    lock.release()
    
# def update_columns_table(table_name: str, db_dir: str, author: str) -> int:
#     lock = file_operations.lock_table(table_name, db_dir)
#     if lock == None:
#         return 0
#     #raise ValueError()
#     start_time = int(time.time())
#     logs = MetaDataStore(db_dir)
#     last_time_id = logs.get_last_table_update(table_name)
#     # read prompts
#     prompts = get_prompts(table_name, db_dir)
#     for name in prompts:
#         changed_columns = get_changed_columns(prompts[name])
#         prompts[name]["changed_columns"] = changed_columns
#         if 'table_creation' in prompts[name]:
#             creation_name = name
#     if last_time_id != 0:
#         old_prompts = get_prompts(table_name, db_dir, time_id=last_time_id)
#         old_prompts_ = {}
#         for name in old_prompts:
#             changed_columns = get_changed_columns(old_prompts[name])
#             old_prompts[name]["changed_columns"] = changed_columns
#             old_prompts_[name] = old_prompts[name]
#     else:
#         old_prompts = None
    
#     row_update = logs.get_last_column_update(table_name, creation_name)
#     mandatory_columns = []
#     if row_update > last_time_id:
#         mandatory_columns += prompts[creation_name]["changed_columns"]
#     to_execute_names = get_executing_prompts(prompts, old_prompts, logs, mandatory_columns, table_name, db_dir, start_time) 
#     update_columns = set()
#     for name in to_execute_names:
#         update_columns = update_columns.union(prompts[name]["changed_columns"])
#     update_columns = list(update_columns)
#     # write to log starting update: 
#     logs.write_to_temp_log(author, table_name, 'start_column_update', int(time.time()), start_time, data=[to_execute_names, update_columns])
#     #raise ValueError()
#     all_columns = file_operations.update_table_columns(update_columns, table_name, db_dir) 
#     logs.write_to_temp_log(author, table_name, 'setup_update_table', int(time.time()), start_time, data=[all_columns])
#     # write update to main
#     for name in to_execute_names:
#         execute_prompt(prompts[name], table_name, start_time, db_dir) # need to do something about all columns (!)
#         logs.write_to_temp_log(author, table_name, 'executed_prompt', int(time.time()), start_time, data=[name])
#     raise ValueError()
#     # write to final update 
#     time_id = file_operations.materialize_table(table_name, db_dir)
#     logs.write_to_update_column_log(author, table_name, time_id, start_time, to_execute_names, update_columns, all_columns)
#     lock.release()
#     return 1

# def update_table_rows(table_name: str, db_dir: str, author: str, new_table: bool = False) -> int:
#     lock = file_operations.lock_table(table_name, db_dir)
#     if lock == 0:
#         return 0
#     start_time = int(time.time())
#     # STEP 1: check dependencies for column changes and setup
#     logs = MetaDataStore(db_dir)
#     table_time = logs.get_last_table_update(table_name)
#     if new_table:
#         time_id = None
#     else:
#         time_id = table_time
#     prompts = get_prompts(table_name, db_dir, time_id=time_id)
#     for name in prompts:
#         if 'table_creation' in prompts[name]:
#             prompt = prompts[name]
#             break
#     changed_columns = get_changed_columns(prompt)
#     prompt["changed_columns"] = changed_columns
#     logs.write_to_temp_log(author, table_name, 'start_row_update', int(time.time()), start_time) #TODO: fix logs
#     # STEP 3: execute prompts
#     execute_prompt(prompt, table_name, start_time, db_dir, time_id = time_id) # need to do something about all columns (!)
#     # STEP 4: write to log?
#     time_id = int(time.time()) 
#     logs.write_to_update_row_log(author, table_name, time_id, start_time) #TODO: Change
#     lock.release()

# def restart_update_columns_table(table_name: str, db_dir: str, author: str) -> None:
#     lock = file_operations.lock_table(table_name, db_dir)
#     logs = MetaDataStore(db_dir)
#     prompts = get_prompts(table_name, db_dir)
#     for name in prompts:
#         changed_columns = get_changed_columns(prompts[name])
#         prompts[name]["changed_columns"] = changed_columns
#     archive_log = logs.get_temp_logs('start_column_update', table_name)
#     archive_log = archive_log[0]
#     # read prompts
#     to_execute_names = archive_log.data[0]
#     update_columns = archive_log.data[1]
#     start_time = archive_log.start_time
#     logs.write_to_temp_log(author, table_name, 'restart_column_update', int(time.time()), start_time)
#     # setup new tables if i didn't already
#     setup_log = logs.get_temp_logs('setup_update_table', table_name)
#     if len(setup_log) == 0:
#         all_columns = file_operations.update_table_columns(update_columns, table_name, db_dir) 
#         logs.write_to_temp_log(author, table_name, 'setup_update_table', int(time.time()), start_time, data=[all_columns])
#     else:
#         setup_log = setup_log[0]
#         all_columns = setup_log.data[0]
#     # write to log starting update: 
#     executed_logs = logs.get_temp_logs('executed_prompt', table_name)
#     executed_names = [log.data[0] for log in executed_logs]
#     logs.write_to_temp_log(author, table_name, 'restart_column_update', int(time.time()), start_time)
#     for name in to_execute_names:
#         if name in executed_names:
#             continue
#         execute_prompt(prompts[name], table_name, start_time, db_dir, threads=prompts[name]['n_threads'])
#         logs.write_to_temp_log(author, table_name, 'executed_prompt', int(time.time()),  data=[name])
#     # write time to archive -> we need to write the update time
#     time_id = file_operations.materialize_table(table_name, db_dir)
#     logs.write_to_update_column_log(author, table_name, time_id, to_execute_names, update_columns, all_columns)
#     lock.release()

# def table_update(table_name:str, db_dir: str, author: str, time_id:Optional[int])->None:
#     if time_id == 0:
#         pass
#     else:
#         pass
#     # update rows
#     # update dependencies -> if we have an update we also look at the origin tables
#     # update columns if relevanr

# def table_copy(table_name:str, db_dir: str, author: str, time_id:int):
#     if time_id == 0:
#         pass
#     else:
#         pass
    

# def clean_up_after_restart(db_dir: str, author: str) -> None:
#     lock = file_operations.lock_table('DATABASE', db_dir)
#     if lock == 0:
#         return 0
#     start_time = time.time()
#     logs = MetaDataStore(db_dir)
#     temp_logs = logs.get_temp_logs()
#     in_progress_tables = []
#     for table_name in temp_logs:
#         if len(temp_logs[table_name]) > 0:
#             in_progress_tables.append(table_name)
    
#     logs.write_to_restart_database_log(author, in_progress_tables, start_time)
#     tables = logs.get_all_tables()
#     for table_name in tables:
#         if len(temp_logs[table_name]) > 0:
#             logs.write_to_temp_log(author, 'DATABASE', 'restart_operation', time.time(), start_time, data = [table_name])
#             if 'start_row_update' in temp_logs[table_name]:
#                 update_rows_table(table_name, db_dir, author)
#             elif 'start_column_update' in temp_logs[table_name]:
#                 restart_update_columns_table(table_name, db_dir, author)
#             logs.write_to_temp_log(author, 'DATABASE', 'restart_operation_end', time.time(), start_time,data = [table_name])
#     logs.write_to_restart_database_log(author, in_progress_tables, start_time, time.time(), clear= True)
#     lock.release()