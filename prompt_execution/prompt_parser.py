
from typing import Any
import file_operations
from meta_operations import MetaDataStore
from collections import deque 
from prompt_parser_table import parse_prompt_from_yaml, parse_obj_from_prompt
import pandas as pd

Prompt = dict[Any]

def get_changed_columns(prompt: Prompt) -> list[str]:
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



def convert_reference(prompt: Prompt) -> Prompt:
    return parse_prompt_from_yaml(prompt)


def get_table_value(item: Any, index: int, cache:dict[str, pd.DataFrame]) -> str:
    return parse_obj_from_prompt(item, index, cache)


def topological_sort(items:list, dependencies:dict)-> list:
    # Step 1: Build the graph and in-degree count
    graph = {}
    in_degree = {item: 0 for item in items}

    for item, deps in dependencies.items():
        for dep in deps:
            if dep not in graph:
                graph[dep] = []
            graph[dep].append(item)
            in_degree[item] += 1

    # Step 2: Initialize the queue with zero in-degree nodes
    queue = deque([item for item in items if in_degree[item] == 0])

    # Step 3: Process the graph
    topo_order = []
    while queue:
        current = queue.popleft()
        topo_order.append(current)

        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # Step 4: Check for cycles
    if len(topo_order) != len(items):
        raise ValueError("Cycle detected! Topological sort not possible.")

    return topo_order

def get_replacement_columns(top_names, prompts:dict[str, Prompt], old_table_id: int, 
                          gen_columns: list[str], logs: MetaDataStore,  
                          table_name: str, db_dir:str, start_time: float) -> list[str]:
    
    to_change_columns = []
    old_prompts = file_operations.get_prompts(table_name, db_dir, old_table_id)
    for name in top_names:
        update = False
        if name not in old_prompts:
            update = True
        else:
            if prompts[name] == old_prompts[name]:
                for dep in prompts[name]['dependencies']:
                    if '.' in dep:
                        dep_table = dep.split('.')[0]
                        dep_col = dep.split('.')[1]
                        if dep_table == table_name or dep_table == 'self':
                            if dep in to_change_columns or dep in gen_columns:
                                update = True
                                break
                        elif logs.get_last_column_update(dep_table, dep_col, start_time) > old_table_id:
                            update = True
                            break
                    else:
                        if logs.get_last_table_update(dep, start_time) > old_table_id:
                            update = True
                            break
            else:
                update = True
        if update:
            to_change_columns.append(prompts[name]["changed_columns"])     
    return to_change_columns


def get_execution_order(prompts: dict[Prompt], table_name: str) -> list[str]:
    dep_graph = {}
    names = []
    top_names = []
    for name in prompts:
        names.append(name)
        for dep in prompts[name]['dependencies']:
            if "." in dep:
                dep_table = dep.split('.')[0]
                dep_col = dep.split('.')[1]
                if dep_table == 'self' or dep_table == table_name:
                    for n in prompts:
                        if dep_col in prompts[n]["parsed_changed_columns"]:
                            if name not in dep_graph:
                                dep_graph[name] = []
                            dep_graph[name].append(n)
    top_names += topological_sort(names, dep_graph)
    return top_names

def get_all_columns(prompts: list[Prompt]) -> list[str]:
    cols = []
    for name in prompts:
       cols.append(prompts[name]['parsed_changed_columns'])

    if len(cols) != len(set(cols)):
        raise ValueError('Changed columns of prompts are not Unique')
    return cols

