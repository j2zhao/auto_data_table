import time

from  auto_data_table import file_operations
from auto_data_table.meta_operations import MetaDataStore
from auto_data_table.parse_prompts.parse_helper import *

def setup_table(table_name: str, db_dir: str, author: str, replace: bool = False) -> None:
    logs = MetaDataStore(db_dir)
    file_operations.setup_table_folder(table_name, db_dir, replace)
    logs.write_to_setup_table_log(author, table_name, time.time(), replace=replace)

def update_columns_table(table_name: str, db_dir: str, author: str) -> int:
    lock = file_operations.lock_table(table_name, db_dir)
    if lock == 0:
        return 0
    start_time = int(time.time())
    logs = MetaDataStore(db_dir)
    last_time_id = logs.get_last_table_update(table_name)
    # read prompts
    prompts = get_prompts(table_name, db_dir)
    for name in prompts:
        changed_columns = get_changed_columns(prompts[name])
        prompts[name]["changed_columns"] = changed_columns
    old_prompts = get_prompts(table_name, db_dir, time_id=last_time_id)
    for name in old_prompts:
        changed_columns = get_changed_columns(old_prompts[name])
        old_prompts[name]["changed_columns"] = changed_columns
    # order prompts -> check dependencies
    to_execute_names = get_executing_prompts(prompts, old_prompts, table_name, db_dir, start_time) 
    update_columns = set(prompts[name]["changed_columns"] for name in to_execute_names)

    # write to log starting update: 
    logs.write_to_temp_log(author, table_name, 'start_column_update', int(time.time()), start_time, data=[to_execute_names, update_columns])
    all_columns = file_operations.update_table_columns(update_columns, table_name, db_dir) 
    logs.write_to_temp_log(author, table_name, 'setup_update_table', int(time.time()), start_time, data=[all_columns])
    # write update to main
    for name in to_execute_names:
        execute_prompt(prompts[name], table_name, start_time, db_dir, threads=prompts[name]['n_threads']) # need to do something about all columns (!)
        logs.write_to_temp_log(author, table_name, 'executed_prompt', int(time.time()), start_time, data=[name])
    # write to final update 
    time_id = file_operations.materialize_table(table_name, db_dir)
    logs.write_to_update_column_log(author, table_name, time_id, start_time, to_execute_names, update_columns, all_columns)
    file_operations.unlock_table(table_name, db_dir)
    return 1

def update_rows_table(table_name: str, db_dir: str, author: str) -> int:
    lock = file_operations.lock_table(table_name, db_dir)
    if lock == 0:
        return 0
    start_time = int(time.time())
    # STEP 1: check dependencies for column changes and setup
    logs = MetaDataStore(db_dir)
    table_time = logs.get_last_table_update(table_name)
    prompts = get_prompts(table_name, db_dir, time_id=table_time)
    for name in prompts:
        changed_columns = get_changed_columns(prompts[name])
        prompts[name]["changed_columns"] = changed_columns
    check = check_external_dependencies(prompts, table_time, logs, table_name, start_time)
    if check:
        raise ValueError("Dependencies Have Updates: Run Column Update Instead.")
    prompts_names = get_prompts_order(prompts, table_name)
    logs.write_to_temp_log(author, table_name, 'start_row_update', int(time.time()), start_time, data=[prompts_names], start=True)
    # STEP 3: execute prompts
    for name in prompts_names:
        # update execute prompt
        execute_prompt(prompts[name], table_name, start_time, db_dir, threads=prompts[name]['n_threads'], time_id = table_time) # need to do something about all columns (!)
        logs.write_to_temp_log(author, table_name, 'executed_prompt', int(time.time()), start_time, data=[name]) 
    # STEP 4: write to log?
    time_id = int(time.time()) 
    logs.write_to_update_row_log(author, table_name, time_id, start_time)
    file_operations.unlock_table(table_name, db_dir)
    return 1

def restart_update_columns_table(table_name: str, db_dir: str, author: str) -> None:
    logs = MetaDataStore(db_dir)
    prompts = get_prompts(table_name, db_dir)
    for name in prompts:
        changed_columns = get_changed_columns(prompts[name])
        prompts[name]["changed_columns"] = changed_columns
    archive_log = logs.get_temp_logs('start_column_update', table_name)
    archive_log = archive_log[0]
    # read prompts
    to_execute_names = archive_log.data[0]
    update_columns = archive_log.data[1]
    start_time = archive_log.start_time
    logs.write_to_temp_log(author, table_name, 'restart_column_update', int(time.time()), start_time)
    # setup new tables if i didn't already
    setup_log = logs.get_temp_logs('setup_update_table', table_name)
    if len(setup_log) == 0:
        all_columns = file_operations.update_table_columns(update_columns, table_name, db_dir) 
        logs.write_to_temp_log(author, table_name, 'setup_update_table', int(time.time()), start_time, data=[all_columns])
    else:
        setup_log = setup_log[0]
        all_columns = setup_log.data[0]
    # write to log starting update: 
    executed_logs = logs.get_temp_logs('executed_prompt', table_name)
    executed_names = [log.data[0] for log in executed_logs]
    logs.write_to_temp_log(author, table_name, 'restart_column_update', int(time.time()), start_time)
    for name in to_execute_names:
        if name in executed_names:
            continue
        execute_prompt(prompts[name], table_name, start_time, db_dir, threads=prompts[name]['n_threads'])
        logs.write_to_temp_log(author, table_name, 'executed_prompt', int(time.time()),  data=[name])
    # write time to archive -> we need to write the update time
    time_id = file_operations.materialize_table(table_name, db_dir)
    logs.write_to_update_column_log(author, table_name, time_id, to_execute_names, update_columns, all_columns)
    file_operations.unlock_table(table_name, db_dir)

def restart_update_rows_table(table_name: str, db_dir: str, author: str) -> None:
    logs = MetaDataStore(db_dir)
    table_time = logs.get_last_table_update(table_name)
    prompts = get_prompts(table_name, db_dir, time_id=table_time)
    for name in prompts:
        changed_columns = get_changed_columns(prompts[name])
        prompts[name]["changed_columns"] = changed_columns
    start_log = logs.get_temp_logs('start_row_update', table_name)
    prompts_names = start_log[0].data[0]
    start_time = start_log[0].start_time
    executed_logs = logs.get_temp_logs('executed_prompt', table_name)
    executed_names = [log.data[0] for log in executed_logs]
    logs.write_to_temp_log(author, table_name, 'restart_row_update', int(time.time()), start_time)
    for name in prompts_names:
        if name in executed_names:
            continue
        execute_prompt(prompts[name], table_name, start_time, db_dir, threads=prompts[name]['n_threads'], time_id=table_time) # need to do something about all columns (!)
        logs.write_to_temp_log(author, table_name, 'executed_prompt', int(time.time()), start_time, data=[name]) 
    # # STEP 4: write to log
    time_id = int(time.time())
    logs.write_to_update_row_log(author, table_name, time_id, start_time)
    file_operations.unlock_table(table_name, db_dir)

def clean_up_after_restart(db_dir: str, author: str) -> None:
    lock = file_operations.lock_table('DATABASE', db_dir)
    if lock == 0:
        return 0
    start_time = time.time()
    logs = MetaDataStore(db_dir)
    temp_logs = logs.get_temp_logs()
    in_progress_tables = []
    for table_name in temp_logs:
        if len(temp_logs[table_name]) > 0:
            in_progress_tables.append(table_name)
    
    logs.write_to_restart_database_log(author, in_progress_tables, start_time)
    tables = logs.get_all_tables()
    for table_name in tables:
        if file_operations.get_table_lock(table_name, db_dir):
            logs.write_to_temp_log(author, 'DATABASE', 'restart_operation', time.time(), start_time, data = [table_name])
            if 'start_row_update' in temp_logs[table_name]:
                restart_update_rows_table(table_name, db_dir, author)
            elif 'start_column_update' in temp_logs[table_name]:
                restart_update_columns_table(table_name, db_dir, author)
            else:
                file_operations.unlock_table(table_name, db_dir)
            logs.write_to_temp_log(author, 'DATABASE', 'restart_operation_end', time.time(), start_time,data = [table_name])
    logs.write_to_restart_database_log(author, in_progress_tables, start_time, time.time(), clear= True)
    file_operations.unlock_table('DATABASE', db_dir)