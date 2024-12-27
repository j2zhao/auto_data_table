import yaml
import re
from meta_operations import MetaDataStore
from parse_helper import TableString, TableReference
import os
from file_operations import get_table
from collections import deque 
from typing import Any, Union

import pandas as pd

def parse_prompt_from_string(val_str: str) -> TableString:
    val_str = val_str.strip()
    if val_str.startswith('<<') and val_str.endswith('>>'):
        return parse_table_reference(val_str[2:-2])
    # Regular expression to match the pattern <<value>>
    pattern = r'<<(.*?)>>'
    # Find all matches
    extracted_values = re.findall(pattern, val_str)
    if len(extracted_values) == 0:
        return val_str
    modified_string = re.sub(pattern, '<<>>', val_str)
    values = []
    for val in extracted_values:
        values.append(parse_table_reference(val))
    table_string =  TableString(modified_string, values)
    return table_string

def process_yaml(data:Any) -> Any:
    if isinstance(data, dict):
        return {k: process_yaml(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [process_yaml(v) for v in data]
    else:
        return parse_prompt_from_string(data)

def parse_prompt(file_path:str) -> Any: 
    with open(file_path, 'r') as file:
        prompt = yaml.safe_load(file)
    prompt = process_yaml(prompt)
    prompt['name'] = os.path.basename(file_path).split('.')[0]
    return prompt


def fetch_table_cach(table_names:list[str], start_time:int, db_dir:str)-> dict[str, pd.DataFrame]:
    cache = {}
    logs = MetaDataStore(db_dir)
    for table_name in table_names:
        table_time = logs.get_last_table_update(table_name, start_time)
        df = get_table(table_name, db_dir, table_time)
        cache[table_name] = df
    return cache

def parse_table_reference(s: str) -> TableReference:
    """
    Parse a string of the form:
    "table_name.column[key_column: value, key_column: value]"
    recursively into a TableReference.
    """
    s = s.strip()

    # Pattern: (table_name.column)([ ... ])?
    main_pattern = r'^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)(\[(.*)\])?$'
    m = re.match(main_pattern, s)
    if not m:
        raise ValueError(f"Invalid TableReference string: {s}")

    main_table = m.group(1)
    main_col = m.group(2)
    inner_content = m.group(4)  # The content inside the brackets if any

    if not inner_content:
        return TableReference(table=main_table, column=main_col, key={})

    pairs = split_top_level_list(inner_content)
    
    key_dict = {}
    for pair in pairs:
        pair = pair.strip()
        kv_split = pair.split(':', 1)
        if len(kv_split) != 2:
            raise ValueError(f"Invalid key-value pair: {pair}")
        key_col = kv_split[0].strip()
        val_str = kv_split[1].strip()
        # Parse the value
        if val_str.startswith("\'") and  val_str.ends("\'"):
            val = val_str
        else:
            val = parse_table_reference(val_str)
        key_dict[key_col] = val

    return TableReference(table=main_table, column=main_col, key=key_dict)

def split_top_level_list(s: str) -> list[str]:
    """
    Split a string by commas that are not nested inside square brackets.
    This is to correctly handle multiple key-value pairs.
    """
    pairs = []
    bracket_depth = 0
    current = []
    for char in s:
        if char == '[':
            bracket_depth += 1
            current.append(char)
        elif char == ']':
            bracket_depth -= 1
            current.append(char)
        elif char == ',' and bracket_depth == 0:
            # top-level comma
            pairs.append(''.join(current))
            current = []
        else:
            current.append(char)
    if current:
        pairs.append(''.join(current))
    return pairs

def read_table_reference(ref:TableReference, index: int, cache: dict,table_name:str, 
                         db_dir: str, keep_list: bool = False)-> Union[str, list[str]]:
    if ref.table == 'self':
        table = table_name
    else:
        table = ref.table
    df = cache[table]
    conditions = {}
    if len(ref.key) == 0:
        conditions['index'] = index
    for condition, value in ref.key:
        if isinstance(value, TableReference):
            value = read_table_reference(value, db_dir, index = index, cache = cache)
            if len(value) > 1:
                raise ValueError("multiple values for {key}")
            value = value[0]
        conditions[condition] = value
    
    query_str = ' & '.join([f'{k} == {repr(v)}' for k, v in conditions.items()])
    rows = df.query(query_str)
    result = rows[ref.column].to_list()    
    if not keep_list and len(result) == 1:
        return result[0]
    else:
        return result
    


def get_dependent_tables(dependencies:list[str], table_name:str) -> list[str]:
    tables = set()
    for dep in dependencies:
        if '.' in dep:
            dep_table = dep.split('.')[0]
            if dep_table == 'self':
                tables.add(table_name)
            else:
                tables.add(dep_table)

        else:
            tables.add(dep)
    return list(tables)

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


def files_are_equal(file1:str, file2:str) -> bool:
    with open(file1, 'rb') as f1, open(file2, 'rb') as f2:
        while True:
            chunk1 = f1.read(4096)  # Read 4KB chunks
            chunk2 = f2.read(4096)
            if chunk1 != chunk2:
                return False
            if not chunk1:  # End of file
                break
    return True

def check_external_dependencies(prompts:dict, table_time: int, 
                                logs: MetaDataStore, table_name:str, start_time: int) -> bool:
    for name in prompts:
        for dep in prompts[name]['dependencies']:
            for dep in prompts[name]['dependencies']:
                if '.' in dep:
                    dep_table = dep.split('.')[0]
                    dep_col = dep.split('.')[1]
                    if dep_table != table_name and dep_table != 'self':    
                        if logs.get_last_column_update(dep_table, dep_col, start_time) > table_time:
                            return True
                else:
                    if logs.get_last_table_update(dep_table, start_time) > table_time:
                        return True
    return False
    

def get_prompts_order(prompts:dict[str, Any], table_name:str) -> list[str]:    
    dep_graph = {}
    names = []
    top_names = []
    for name in prompts:
        if prompts[name]["table_creation"] == True:
            top_names.append(name)
        else:
            names.append(name)
            for dep in prompts[name]['dependencies']:
                if "." in dep:
                    dep_table = dep.split('.')[0]
                    dep_col = dep.split('.')[1]
                    if dep_table == 'self' or dep_table == table_name:
                        for n in prompts:
                            if dep_col in prompts[n]["changed_columns"]:
                                if name not in dep_graph:
                                    dep_graph[name] = []
                                dep_graph[name].append(n)
    top_names += topological_sort(names, dep_graph)
    return top_names

