
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


def _topological_sort(items: list, dependencies: dict) -> list:
    # Step 1: Build the graph based on parent -> child dependencies
    graph = {item: [] for item in items}
    
    for parent, children in dependencies.items():
        for child in children:
            if child not in graph:
                graph[child] = []  # Ensure child exists in graph
            graph[parent].append(child)  # Parent points to its children
    
    # Step 2: Perform DFS-based topological sorting
    visited = set()  # To track visited nodes
    visiting = set()  # To track the current recursion stack (for cycle detection)
    sorted_order = []

    def dfs(node):
        if node in visiting:
            raise ValueError("Cycle detected! Topological sort not possible.")
        if node in visited:
            return

        visiting.add(node)  # Mark node as visiting
        for child in graph[node]:
            dfs(child)  # Visit children first
        visiting.remove(node)  # Remove from visiting
        visited.add(node)  # Mark node as visited
        sorted_order.append(node)  # Add node after processing children

    # Step 3: Apply DFS to all items
    for item in items:
        if item not in visited:
            dfs(item)
    # Return the sorted order directly (no need to reverse because we're appending after dependencies)
    return sorted_order


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

InternalDeps = dict[str, list[str]]
ExternalDeps = dict[str, list[tuple[str, str, str, float, bool]]]

def _parse_dependencies(prompts:dict[Prompt], table_generator:str,
                        start_time: float, db_metadata:MetaDataStore) -> tuple[InternalDeps, ExternalDeps]:

    external_deps = {}
    internal_prompt_deps = {}
    internal_deps = {}
    gen_columns = prompts[table_generator]['parsed_changed_columns']
    for pname in prompts:
        external_deps[pname] = set()
        if pname != table_generator:
            internal_deps[pname] = set(gen_columns)
            internal_prompt_deps[pname] = {table_generator}
        else:
            internal_deps[pname] = set()
            internal_prompt_deps[pname] = set()

        for dep in prompts[pname]['dependencies']:
            table, column, instance = parse_string(dep)
            if instance == None:
                latest = True
            else:
                latest = False
            if table == 'self':
                internal_deps[pname].add(column) #TODO check this
                for pn in prompts:
                    if column in prompts[pn]['parsed_changed_columns']:
                        internal_prompt_deps[pname].add(pn)
                continue
            elif not db_metadata.get_table_multiple(table) and instance != None:
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
            external_deps[pname].add((table, column, instance, mat_time, latest))
        external_deps[pname] = list(external_deps[pname])
        internal_deps[pname] = list(internal_deps[pname])
        internal_prompt_deps[pname] = list(internal_prompt_deps[pname])
    return internal_prompt_deps, internal_deps, external_deps

def parse_prompts(prompts: dict[Prompt], db_metadata: MetaDataStore , start_time:float,table_name:str, db_dir: str):
    metadata = prompts['description']
    del prompts['description']
    

    for pname, prompt in prompts.items():
        if 'parsed_changed_columns' not in prompt:
            prompt['parsed_changed_columns'] = get_changed_columns(prompt) # need to deal with?
    
    internal_prompt_deps, internal_deps, external_deps = _parse_dependencies(prompts, metadata['table_generator'], start_time, db_metadata)
    top_pnames = _topological_sort(list(prompts.keys()), internal_prompt_deps)
    all_columns = []
    to_change_columns = []
    for i, pname in enumerate(top_pnames):
        all_columns += prompts[pname]['parsed_changed_columns']

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
                to_change_columns += prompts[pname]['parsed_changed_columns']
    else:
        #to_change_columns = all_columns
        for pname in top_pnames[1:]:
            to_change_columns += prompts[pname]['parsed_changed_columns']
    return top_pnames, to_change_columns, all_columns, internal_deps, external_deps
