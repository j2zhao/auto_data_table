
import pandas as pd
import threading
from typing import Optional
import openai
import ast
from concurrent.futures import ThreadPoolExecutor

from auto_data_table import file_operations
from auto_data_table.llm_functions.open_ai_thread import Open_AI_Thread, add_open_ai_secret
from auto_data_table.prompt_execution import prompt_parser 
from auto_data_table.prompt_execution import llm_prompts


def _execute_llm(index: int, prompt: dict, client: Optional[openai.OpenAI], 
                 lock: threading.Lock, cache: dict[str, pd.DataFrame], 
                 table_name:str, db_dir: str) -> None:
    df = cache[table_name]
    to_change = False
    for i, column in enumerate(prompt['changed_columns']):
        if df.at[index, column] == '':
            to_change = True
    if not to_change:
        return 
    # get open_ai file keys
    name = prompt['name'] + str(index)
    prompt['context_files'] = prompt_parser.get_table_value(prompt['context_files'], index,cache)
    prompt['context_msgs'] = prompt_parser.get_table_value(prompt['context_msgs'], index, cache)
    prompt['instructions'] = prompt_parser.get_table_value(prompt['instructions'], index,cache)
    prompt['questions'] = prompt_parser.get_table_value(prompt['questions'], index, cache)

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
            df.at[index, column]= results[i]
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
    lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        executor.map(
            lambda i: _execute_llm(i, prompt, client, lock, cache, table_name, db_dir),
            indices
        )