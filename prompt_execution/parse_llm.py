
import pandas as pd
import threading
from typing import Optional
import openai
import ast
from concurrent.futures import ThreadPoolExecutor

from auto_data_table import file_operations
from auto_data_table.llm_functions.open_ai_thread import Open_AI_Thread, add_open_ai_secret
from auto_data_table.prompt_execution import parse_helper
from auto_data_table.prompt_execution import llm_prompts

lock = threading.Lock()


def _execute_llm(prompt: dict, client: Optional[openai.OpenAI], cache: dict[str, pd.DataFrame], 
                 index: int, table_name:str, db_dir: str) -> None:
    global df
    to_change = False
    for i, column in enumerate(prompt['changed_columns']):
        if df.iloc[index, df.columns.get_loc(column)] == '':
            to_change = True
    if not to_change:
        return 
    # get open_ai file keys
    name = prompt['name'] + str(index)
    prompt['context_files'] = parse_helper.parse_prompt_to_str(prompt['context_files'], index,cache, table_name, db_dir)
    prompt['context_msgs'] = parse_helper.parse_prompt_to_str(prompt['context_msgs'], index, cache, table_name, db_dir)
    prompt['instructions'] = parse_helper.parse_prompt_to_str(prompt['instructions'], index,cache, table_name, db_dir)
    prompt['questions'] = parse_helper.parse_prompt_to_str(prompt['questions'], index, cache, table_name, db_dir)

    uses_files = len(prompt['context_files']) > 0
    thread = Open_AI_Thread(name, prompt['model'], prompt['temperature'], prompt['retry'],
                            prompt['instructions'], client=client, uses_files = uses_files)

    if isinstance(prompt['context_msgs'], list):
        for i, cfile in enumerate(prompt['context_files']):
            thread.add_message(message = prompt['context_msgs'][i],  file_ids = [cfile])
    else:
        thread.add_message(prompt['context_msgs'], file_ids = prompt['context_files'])

    if prompt['output_type'] == 'category' and 'category_definition' in prompt:
        thread.add_message(prompt['category_definition'])

    # parse and add questions
    results = []
    for question in prompt['questions']:
        if prompt['output_type'] == 'category':
            question = question.replace('CATEGORIES', prompt['category_names'])        
        thread.add_message(question)        
        result = thread.run_query()
        results.append(result)            
    
    # deal with output_types:
    if prompt['output_type'] == 'freeform':
        pass
    elif prompt['output_type'] == 'entity':
        msg = llm_prompts.ENTITY_MSG
        msg = msg.replace('ENTITY_NAME', prompt['entity_name'])
        thread.add_message(message = msg)
        result = thread.run_query()
        results.append(result)
    elif prompt['output_type'] == 'entity_list':
        msg = llm_prompts.ENTITY_LIST_MSG
        msg = msg.replace('ENTITY_NAME', prompt['entity_name'])
        thread.add_message(message = msg)
        for i in range(prompt['retry']):
            result = thread.run_query()
            try:
                result = ast.literal_eval(result)
                break
            except:
                print('Could not convert to Python list') #TODO: better logging statements
            results.append(result)

    elif prompt['output_type'] == 'category':
        msg = llm_prompts.CATEGORY_MSG
        msg = msg.replace('CATEGORIES', prompt['category_names'])
        thread.add_message(message = msg)
        result = thread.run_query()
        results.append(result)
    else:
        raise ValueError('Output type not supported')
    
    with lock:
        for i, column in enumerate(prompt['changed_columns']):
            df.iloc[index, df.columns.get_loc(column)] = results[i]
        file_operations.write_table(df, table_name, db_dir)


def execute_llm_from_prompt(prompt:dict, columns: list, cache: dict, n_threads: int,
                            table_name:str, db_dir:str, time_id: Optional[int]) -> None:
    '''Only support OpenAI Thread prompts for now'''
    key_file =  prompt['open_ai_key']
    
    with open(key_file, 'r') as f:
        secret = f.read()
        add_open_ai_secret(secret)
    client = openai.OpenAI()
    indices = list(range(len(cache[table_name])))
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = [
            executor.submit(_execute_llm, prompt, client, cache, i, columns,  table_name, db_dir) for i in indices
        ]
        for future in futures:
            future.result()  # Wait for completion and raise exceptions if any
