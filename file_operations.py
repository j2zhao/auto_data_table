import os
import shutil
import pandas as pd
import json
from typing import Optional, Any
import yaml


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

    meta_dir = os.path.join(db_dir, 'locks')    
    os.makedirs(meta_dir)

    with open(os.path.join(meta_dir, 'log.txt'), "w") as file:
        pass

    with open(os.path.join(meta_dir, 'active_log.json'), "w") as file:
        json.dump({}, file)

    with open(os.path.join(meta_dir, 'columns_history.json'), "w") as file:
        json.dump({}, file)  
    
    with open(os.path.join(meta_dir, 'tables_history.json'), "w") as file:
        json.dump({}, file)  

def setup_temp_table(table_name: str, db_dir: str, prev_name_id: Optional[str] = None,
                     prompts: list[str] = [], temp_name: Optional[str] = None,
                    gen_prompt: str = '') -> None:
    
    #TODO: conditions on naming temp (!)
    if not temp_name:
        temp_name = 'TEMP'
    elif not temp_name.startswith('TEMP'):
        raise ValueError('Temp folder name has to start with "TEMP"')
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, temp_name)
    if os.path.exists(temp_dir):
        print(f"{temp_name} folder already exists for {table_name}")
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    # create or copy promtpts
    prompt_dir = os.path.join(temp_dir, 'prompts')
    current_table_path = os.path.join(temp_dir, 'table.csv')
    metadata_path = os.path.join(prompt_dir, 'metadata.yaml')        
    
    if prev_name_id != None:
        prev_dir = os.path.join(table_dir, str(prev_name_id))
        prev_prompt_dir = os.path.join(prev_dir, 'prompts')
        shutil.copytree(prev_prompt_dir, prompt_dir, copy_function=shutil.copy2)
        prev_table_path = os.path.join(prev_dir, 'table.csv')
        shutil.copy2(prev_table_path, current_table_path)
        with open(metadata_path, 'r') as file:
            metadata = yaml.safe_load(file)
        metadata['origin'] = prev_name_id
        with open(metadata_path, 'w') as file:
            yaml.safe_dump(metadata, file)
    
    elif len(prompts) != 0:
        os.makedirs(prompt_dir)
        df = pd.DataFrame()
        df.to_csv(current_table_path, index=False)
        prompt_dir_ = os.path.join(table_dir, 'prompts')
        for prompt in prompts:
            prompt_path_ = os.path.join(prompt_dir_, prompt + '.yaml')
            prompt_path = os.path.join(prompt_dir, prompt + '.yaml')
            shutil.copy2(prompt_path_, prompt_path)
        metadata = {'table_generator': gen_prompt}
        metadata['copied_prompts'] = prompts
        with open(metadata_path, 'w') as file:
            yaml.safe_dump(metadata, file) 
    else:
        os.makedirs(prompt_dir)
        df = pd.DataFrame()
        df.to_csv(current_table_path, index=False)
        with open(metadata_path, 'w') as file:
            pass

def get_table_status(table_name, db_dir):
    table_dir = os.path.join(db_dir, table_name)
    meta_path = os.path.join(table_dir, 'metadata.yaml')
    with open(meta_path, 'r') as f:
        metadata = yaml.safe_load(f)
    return metadata['allow_multiple_versions']

def setup_table_folder(table_name: str, db_dir: str, allow_multi:bool) -> None:
    if not table_name.isalnum():
        raise ValueError('Table Name Needs To Be Alphanumeric')
    if table_name == 'DATABASE' or table_name == 'TABLE':
        raise ValueError(f'Special Name Taken: {table_name}.')
    table_dir = os.path.join(db_dir, table_name)
    if os.path.isdir(table_dir):
        shutil.rmtree(db_dir)
    if os.path.isfile(table_dir):
        os.remove(table_dir)
    os.makedirs(table_dir)
    prompt_dir = os.path.join(table_dir, 'prompts')
    os.makedirs(prompt_dir)
    metadata = {}
    metadata['allow_multiple_versions'] = allow_multi
    meta_path = os.path.join(table_dir, 'metadata.yaml')
    with open(meta_path, 'w') as f:
        yaml.safe_dump(metadata, f)


def materialize_table(table_id:str, table_name:str, db_dir: str) -> int:
    table_dir = os.path.join(db_dir, table_name)
    temp_dir = os.path.join(table_dir, 'TEMP')
    if not os.path.exists(temp_dir):
        raise ValueError("No Table In Progress")
        
    new_dir = os.path.join(table_dir, table_id)
    os.rename(temp_dir, new_dir)
    return table_id

def delete_lock(table_name: str, db_dir: str, table_id:Optional[str] = None):
    if table_id == None:
        lock_dir = os.path.join(db_dir, 'locks', table_name)
        if os.path.exists(lock_dir):
            shutil.rmtree(lock_dir)
    else:
        lock_dir = os.path.join(db_dir, 'locks', table_name, f'{table_id}.lock')
        if os.path.exists(lock_dir):
            os.remove(lock_dir)


def delete_table(table_name: str, db_dir: str, table_id: Optional[str] = None):
    table_dir = os.path.join(db_dir, table_name)
    if table_id != None:
        table_dir = os.path.join(table_dir, str(table_id))
    if os.path.isdir(table_dir):
        shutil.rmtree(table_dir)

# get table
def get_table(table_id: str, table_name: str, db_dir: str, rows: Optional[int] = None) -> pd.DataFrame:
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, table_id)
    table_dir = os.path.join(table_dir, 'table.csv')
    try:
        df = pd.read_csv(table_dir, nrows=rows) 
        return df
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
     
def get_prompt(table_id:str, name: str, table_name: str, db_dir: str) -> dict:
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, table_id)
    meta_path = os.path.join(table_dir, 'prompts')
    meta_path = os.path.join(meta_path, f'{name}.yaml')
    with open(meta_path, 'r') as file:
        return yaml.safe_load(file)

def write_table(df: pd.DataFrame, table_id:str, table_name: str, db_dir: str) -> None:
    if 'pos_index' in df.columns:
        df.drop(columns="pos_index", inplace=True)
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, table_id)
    table_dir = os.path.join(table_dir, 'table.csv')
    df = df.to_csv(table_dir, index=False)

def write_prompt(name: str, prompt: dict, table_name: str, db_dir: str) -> None:
    table_dir = os.path.join(db_dir, table_name)
    table_dir = os.path.join(table_dir, 'TEMP')
    meta_path = os.path.join(table_dir, 'prompts')
    meta_path = os.path.join(meta_path, f'{name}.yaml')
    with open(meta_path, 'w') as file:
        yaml.safe_dump(prompt, file)

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