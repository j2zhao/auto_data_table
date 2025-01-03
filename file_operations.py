import os
import shutil
import pandas as pd
import json
import time
from filelock import FileLock
import threading
from typing import Optional, Any
import yaml


def setup_database(db_dir: str, replace: bool = False) -> None:
    # TODO: setup prompt table (!)
    # we basically have a folder that just gathers prompts
    # table_name, table_instance, prompt_name, prompt

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
        pass

    with open(os.path.join(meta_dir, 'temp_log.json'), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, 'columns_history.json'), "w") as file:
        json.dump({}, file)  


def setup_temp_table(table_name: str, db_dir: str, prev_time_id: Optional[int] = None) -> None:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, 'TEMP')
    if os.path.exists(temp_dir):
        print(f"TEMP folder already exists for {table_name}")
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    prompt_dir = os.path.join(temp_dir, 'prompts')
    current_table_path = os.path.join(temp_dir, 'table.csv')
    metadata_path = os.path.join(prompt_dir, 'metadata.yaml')
    if prev_time_id == None:
        os.makedirs(prompt_dir)
        df = pd.DataFrame()
        df.to_csv(current_table_path, index=False)
        with open(metadata_path, 'w') as file:
            yaml.dump({}, file)
    else:
        prev_dir = os.path.join(table_dir, str(prev_time_id))
        prev_prompt_dir = os.path.join(prev_dir, 'prompts')
        shutil.copytree(prev_prompt_dir, prompt_dir, copy_function=shutil.copy2)
        prev_table_path = os.path.join(prev_dir, 'table.csv')
        shutil.copy2(prev_table_path, current_table_path)
        with open(metadata_path, 'r') as file:
            metadata = yaml.load(file)
        metadata['origin'] = prev_time_id
        with open(metadata_path, 'w') as file:
            yaml.dump(metadata, file)
    

def setup_table_folder(table_name: str, db_dir: str) -> None:
    if table_name == 'DATABASE':
        raise ValueError('Special Name Taken: DATABASE.')
    table_dir = os.path.join(db_dir, table_name)
    # if not replace and os.path.exists(table_dir):
    #     raise FileExistsError('Path already taken')
    if os.path.isdir(table_dir):
        shutil.rmtree(db_dir)
    if os.path.isfile(table_dir):
        os.remove(table_dir)
    os.makedirs(table_dir)
    lock_path = os.path.join(table_dir, 'WRITE.lock')
    FileLock(lock_path)
    setup_temp_table(table_name, db_dir)

def materialize_table(table_name:str, db_dir: str) -> int:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, 'TEMP')
    if not os.path.exists(temp_dir):
        raise ValueError("No Table In Progress")
    time_id = time.time() + 1
    new_dir = os.path.join(table_dir, str(time_id))
    os.rename(temp_dir, new_dir)
    return time_id

def lock_database(db_dir:str) -> Optional[dict[str, FileLock]]:
    table_dir = os.path.join(db_dir, 'metadata')
    lock_path = os.path.join(table_dir, 'WRITE.lock')
    lock = FileLock(lock_path)
    lock.acquire()
    locks = {}
    locks['DATABASE'] = lock
    for tname in os.listdir(db_dir):
        table_dir = os.path.join(db_dir, tname) 
        l_path = os.path.join(table_dir, 'WRITE.lock')
        if os.path.exists(l_path):
            locks[lock] = lock.acquire()
    return locks


def unlock_database(locks: dict[str, FileLock]):
    for lock in locks:
        if lock != 'DATABASE':
            locks[lock].release()
    locks['DATABASE'].release()

def lock_table(table_name: str, db_dir: str, blocking: bool = True) -> Optional[FileLock]:
    if table_name != 'DATABASE':
        table_dir = os.path.join(db_dir, table_name)
    else:
        table_dir = os.path.join(db_dir, 'metadata')
    lock_path = os.path.join(table_dir, 'WRITE.lock')
    lock = FileLock(lock_path)
    if not blocking:
        success = lock.acquire(blocking=blocking)
        if not success:
            return None
        else:
            return lock
    else:
        lock.acquire(blocking=blocking)
        return lock

def delete_table(table_name: str, db_dir: str, time_id: Optional[int] = None):
    table_dir = os.path.join(db_dir, table_name)
    if time_id != None:
        table_dir = os.path.join(table_dir, str(time_id))
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)

# get table
def get_table(table_name: str, db_dir: str, time: Optional[int] = None, rows: Optional[int] = None) -> pd.DataFrame:
    table_dir = os.path.join(db_dir, table_name)
    if time != None:
        table_dir = os.path.join(table_dir, str(time))
    else:
        table_dir = os.path.join(table_dir, 'TEMP')
    table_dir = os.path.join(table_dir, 'table.csv')
    try:
        df = pd.read_csv(table_dir, nrows=rows) 
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
     
def get_prompt(name: str, table_name: str, db_dir: str, time: Optional[int] = None) -> dict:
    table_dir = os.path.join(db_dir, table_name)
    if time != None:
        table_dir = os.path.join(table_dir, str(time))
    else:
        table_dir = os.path.join(table_dir, 'TEMP')
    meta_path = os.path.join(table_dir, 'prompts')
    meta_path = os.path.join(meta_path, f'{name}.yaml')
    with open(meta_path, 'r') as file:
        return yaml.load(file)

def write_table(df: pd.DataFrame, table_name: str, db_dir: str) -> None:
    if 'pos_index' in df.columns:
        df.drop(columns="pos_index", inplace=True)
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, 'TEMP')
    table_dir = os.path.join(table_dir, 'table.csv')
    df = df.to_csv(table_dir, index=False)

def write_prompt(name: str, prompt: dict, table_name: str, db_dir: str) -> None:
    table_dir = os.path.join(db_dir, table_name)
    if time != None:
        table_dir = os.path.join(table_dir, str(time))
    else:
        table_dir = os.path.join(table_dir, 'TEMP')
    meta_path = os.path.join(table_dir, 'prompts')
    meta_path = os.path.join(meta_path, f'{name}.yaml')
    with open(meta_path, 'w') as file:
        yaml.dump(prompt, file)

def get_table_versions(db_dir: str) -> dict[str, list[int]]:
    versions = {}
    for tname in os.listdir(db_dir):
        tpath_ = os.path.join(db_dir, tname)
        if tname != 'metadata' and os.path.isdir(tpath_):
            for vname in os.listdir(tpath_):
                vpath_ = os.path.join(tpath_, vname)
                if os.path.isdir(vpath_) and str.isdigit(vname):
                    if tname not in versions:
                        versions[tname] = []
                    versions[tname].append(vname)
    return versions
                        

def get_prompts(table_name:str, db_dir: str, time_id: Optional[int] = None) -> dict[str, Any]:
    table_dir = os.path.join(db_dir, table_name)
    if time_id != None:
        temp_dir = os.path.join(table_dir, time_id)
    else:
        temp_dir = os.path.join(table_dir, 'TEMP')
    prompt_dir = os.path.join(temp_dir, 'prompts')
    prompts = {}
    for item in os.listdir(prompt_dir):
        if item.endswith('.yaml'):
            name = item.split('.')[0]
            prompt_path = os.path.join(prompt_dir, item)
            with open(prompt_path, 'r') as file:
                prompt = yaml.safe_load(file)
                prompt['name'] = name
            prompts[name] = prompt
    return prompts



def check_folder(table_name:str, db_dir: str, time_id: Optional[int] = None) -> bool:
    table_dir = os.path.join(db_dir, table_name)
    if time_id != None:
        table_dir = os.path.join(table_dir, str(time_id))
    return os.path.exists(table_dir)