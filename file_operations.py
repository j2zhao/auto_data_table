import os
import shutil
import pandas as pd
import json
import time
from filelock import FileLock
import threading
from typing import Optional


def setup_database(db_dir: str, replace: bool = False) -> None:
    if not replace and os.path.exists(db_dir):
        raise FileExistsError('path already taken')
    elif replace and os.path.isdir(db_dir):
        shutil.rmtree(db_dir)
    elif replace and os.path.isfile(db_dir):
        os.remove(db_dir)

    os.makedirs(db_dir)
    os.makedirs(os.path.join(db_dir, 'code_functions'))
    meta_dir = os.path.join(db_dir, 'metadata')
    os.makedirs(meta_dir)

    with open(os.path.join(meta_dir, 'log.json'), "w") as file:
        json.dump([], file)

    with open(os.path.join(meta_dir, 'temp_log.json'), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, 'columns_history.json'), "w") as file:
        json.dump({}, file)  

    with open(os.path.join(meta_dir, 'UPDATE.json'), "w") as file:
        json.dump(False, file)

def setup_temp_table(table_name: str, db_dir: str, prev_time_id: Optional[int] = None) -> None:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, 'TEMP')
    if os.path.exists(temp_dir):
        raise ValueError(f"TEMP folder already exists for {table_name}")
    os.makedirs(temp_dir)
    prompt_dir = os.path.join(temp_dir, 'prompts')
    if prev_time_id == None:
        os.makedirs(prompt_dir)
    else:
        prev_dir = os.path.join(table_dir, str(prev_time_id))
        prev_dir = os.path.join(prev_dir, 'prompts')
        shutil.copytree(prev_dir, prompt_dir, copy_function=shutil.copy2)

def setup_table_folder(table_name: str, db_dir: str, replace: bool = False) -> None:
    if table_name == 'DATABASE':
        raise ValueError('Special Name Taken: DATABASE.')
    table_dir = os.path.join(db_dir, table_name)
    if not replace and os.path.exists(table_dir):
        raise FileExistsError('path already taken')
    elif replace and os.path.isdir(table_dir):
        shutil.rmtree(db_dir)
    elif replace and os.path.isfile(table_dir):
        os.remove(table_dir)
    os.makedirs(table_dir)
    with open(os.path.join(table_dir, 'UPDATE.json'), "w") as file:
        json.dump(False, file)
    setup_temp_table(table_name, db_dir)


def materialize_table(table_name:str, db_dir: str) -> int:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, 'TEMP')
    if not os.path.exists(temp_dir):
        raise ValueError("No Table In Progress")
    time_id = time.time() + 1
    new_dir = os.path.join(table_dir, str(time_id))
    os.rename(temp_dir, new_dir)
    setup_temp_table(table_name, db_dir, prev_time_id = time_id)
    return time_id

def lock_table(table_name: str, db_dir: str) -> bool:
    if table_name != 'DATABASE':
        table_dir = os.path.join(db_dir, table_name)
    else:
        table_dir = os.path.join(db_dir, 'metadata')
    lock_path = os.path.join(table_dir, 'UPDATE.temp')
    lock = FileLock(lock_path)
    with lock:
        with open(lock_path, 'r') as f:
            lock = json.load(f)
            if lock:
                return 0
            else:
                json.dump(True, f)
                return 1
    
def unlock_table(table_name:str, db_dir: str) -> None:
    if table_name != 'DATABASE':
        table_dir = os.path.join(db_dir, table_name)
    else:
        table_dir = os.path.join(db_dir, 'metadata')
    lock_path = os.path.join(table_dir, 'UPDATE.temp')
    lock = FileLock(lock_path)
    with lock:
        with open(lock_path, 'r') as f:
            lock = json.load(f)
            if not lock:
                return 0
            else:
                json.dump(False, f)
                return 1
    
def get_table_lock(table_name: str, db_dir: str) -> bool:
    if table_name != 'DATABASE':
        table_dir = os.path.join(db_dir, table_name)
    else:
        table_dir = os.path.join(db_dir, 'metadata')
    lock_path = os.path.join(table_dir, 'UPDATE.temp')
    with open(lock_path, 'r') as f:
        return json.load(f)

# get table
def get_table(table_name: str, db_dir: str, time: Optional[int] = None, rows: Optional[int] = None) -> pd.DataFrame:
    table_dir = os.path.join(db_dir, table_name)
    if time != None:
        table_dir = os.path.join(table_dir, str(time))
    else:
        table_dir = os.path.join(table_dir, 'TEMP')
    table_dir = os.path.join(table_dir, 'table.csv')
    df = pd.read_csv(table_dir, nrows=rows) 
    return df

def write_table(df: pd.DataFrame, table_name: str, db_dir: str, 
                lock: Optional[threading.Lock] = None, time_id: Optional[int] = None) -> None:
    if 'pos_index' in df.columns:
        df.drop(columns="pos_index", inplace=True)
    table_dir = os.path.join(db_dir, table_name)
    if time_id == None:
        table_dir = os.path.join(table_dir, 'TEMP')
    else:
        table_dir = os.path.join(table_dir, str(time_id))
    table_dir = os.path.join(table_dir, 'table.csv')
    if lock:
        with lock:
            df.to_csv(table_dir, index=False)
    else:
        df = df.to_csv(table_dir, index=False)

def update_table_columns(columns: list, table_name: str, db_dir: str, replace: bool = True) -> list[str]:
    df = get_table(table_name, db_dir)
    for col in columns:
        if replace or col not in df.columns:
            df[col] = ''
    write_table(df, table_name, db_dir)
    return list(df.columns)





