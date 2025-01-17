
from typing import Any, Union
import file_operations
from meta_operations import MetaDataStore
from collections import deque 
from auto_data_table.prompt_execution.prompt_parser_table import parse_prompt_from_yaml, parse_obj_from_prompt
import pandas as pd
import copy 
import re

Prompt = dict[Any]
Cache = dict[Union[str, tuple[str, str]], pd.DataFrame]

def get_changed_columns(prompt: Prompt) -> list[str]:
    if prompt['type'] == 'code':
        changed_columns =  copy.deepcopy(prompt['changed_columns'])
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


def _topological_sort(items:list, dependencies:dict)-> list:
    # Step 1: Build the graph and in-degree count
    graph = {}
    graph = {item: [] for item in items}
    in_degree = {item: 0 for item in items}

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



def parse_string(input_string):
    # Define the regex pattern for all cases
    pattern = r"^(\w+)(?:\.(\w+))?(?:\((\w+)\))?$"
    match = re.match(pattern, input_string)

    if match:
        # Extract components
        part1 = match.group(1)  # First ALPHANUMERIC
        part2 = match.group(2)  # Second ALPHANUMERIC (optional)
        part3 = match.group(3)  # ALPHANUMERIC inside parentheses (optional)
        return part1, part2, part3
    else:
        raise ValueError("Input string does not match the expected format.")

InternalDeps = dict[str[set[str]]]
ExternalDeps = dict[str[set[tuple[str, str, str, float]]]]

def _parse_dependencies(prompts:dict[Prompt], table_generator:str,
                        start_time: float, db_metadata:MetaDataStore) -> tuple[InternalDeps, ExternalDeps]:

    external_deps = {}
    internal_prompt_deps = {}
    for pname in prompts:
        external_deps[pname] = set()
        internal_prompt_deps[pname] = set(table_generator)

        for dep in prompts[pname]['dependencies']:
            table, column, instance = parse_string(dep)
            if table == 'self':
                for pn in prompts:
                    if column in prompts[n]["parsed_changed_columns"]:
                        internal_prompt_deps[pname].append(pn)
            
            elif not db_metadata.get_table_status(table) and instance != None:
                raise ValueError(f"Table dependency ({table}, {column}, {instance}) for prompt {pname} doesn't have versions.")
            elif column != None:
                if instance != None:
                    mat_time = db_metadata.get_column_version_update(column, instance, table, start_time)
                else:
                    instance, mat_time = db_metadata.get_last_column_update(table, column, start_time)
                if mat_time == 0:
                    raise ValueError('Table dependency ({table}, {column}, {instance}) for prompt {pname} not materialized at {start_time}')
            else:
                if instance != None:
                    mat_time = db_metadata.get_table_version_update(instance, table, start_time)
                else:
                    instance, mat_time = db_metadata.get_last_table_update(table, start_time)   
                if mat_time == 0:
                    raise ValueError('Table dependency ({table}, {column}, {instance}) for prompt {pname} not materialized at {start_time}')
            external_deps[pname].add((table, column, instance, mat_time))
    return internal_prompt_deps, external_deps

def parse_prompts(prompts: dict[Prompt], db_metadata: MetaDataStore , start_time:float,table_name:str, db_dir: str):
    metadata = prompts['metadata']
    del prompts['metadata']
    

    for pname, prompt in prompts.items():
        if 'parsed_changed_columns' not in prompt:
            prompt['parsed_changed_columns'] = get_changed_columns(prompt) # need to deal with?
    
    internal_prompt_deps, external_deps = _parse_dependencies(prompts, metadata['table_generator'], start_time, db_metadata)

    top_pnames = _topological_sort(list(prompts.keys), internal_prompt_deps)
    # TODO: get all columns
    # TODO: get changed columns
    all_columns = []
    to_change_columns = []
    for pname in top_pnames[1:]:
        all_columns.append(prompts[pname]['parsed_changed_columns'])

    if 'prev_name_id' in metadata:
        to_execute = []
        prev_start_time = metadata['prev_start_time']
        prev_name_id = metadata['prev_name_id']
        to_execute = [top_pnames[0]] # we always run the generator

        prev_prompts = file_operations.get_prompts(table_name, db_dir, prev_name_id)
        for pname in top_pnames[1:]:
            execute = False
            for dep in internal_prompt_deps[pname]:
                if dep in to_execute and dep != top_pnames[0]:
                    execute = True
                    break
            if not execute:
                for dep in external_deps[pname]:
                    if dep[3] >= prev_start_time:
                        to_execute.append(pname)
                        execute = True
                        break
            if not execute:
                if pname not in prev_prompts:
                    execute = True
                elif prev_prompts[pname] != prompts[pname]:
                    execute = True
            if execute:
                to_execute.append(pname)
                to_change_columns.append(prompts[pname]['parsed_changed_columns'])
    else:
        to_change_columns = all_columns
    return top_pnames, to_change_columns, all_columns, internal_prompt_deps, external_deps
