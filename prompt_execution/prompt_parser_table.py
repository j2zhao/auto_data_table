
from typing import Any, Optional, Union
import re
from dataclasses import dataclass
import pandas as pd

@dataclass
class TableReference:
    table: str
    column: str
    key: Optional[dict[str, Union["TableReference", str]]] = None
    

@dataclass
class TableString:
    text: str
    references: list[TableReference]

def parse_prompt_from_yaml(data:Any, table_name: str) -> Any:
    if isinstance(data, dict):
        return {k: parse_prompt_from_yaml(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [parse_prompt_from_yaml(v) for v in data]
    elif isinstance(data, str):
        return parse_prompt_from_string(data, table_name)
    else:
        return data
    
def parse_obj_from_prompt(prompt:Any, index:int, cache:dict[str, pd.DataFrame]) -> Any:
    if isinstance(prompt, TableString):
        prompt_ = prompt.text
        for ref in prompt.references: 
            ref_ = read_table_reference(ref, index=index, cache= cache)
            prompt_ = prompt_.replace('<<>>', ref_, 1)
    elif isinstance(prompt, TableReference):
        prompt_ = read_table_reference(prompt, index=index, cache= cache)
    elif isinstance(prompt, dict):
        prompt_ = {}
        for key in prompt:
            temp = read_table_reference(prompt[key], index=index, cache= cache)
            prompt_[key] = temp
    
    elif isinstance(prompt, list):
        prompt_ = []
        for val in prompt:
            temp = read_table_reference(val, index=index, cache= cache)
            prompt_.append(temp)
    else:
        prompt_ = prompt
    return prompt_

def parse_prompt_from_string(val_str: str, table_name:str) -> TableString:
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
        values.append(parse_table_reference(val, table_name))
    table_string =  TableString(modified_string, values)
    return table_string



def parse_table_reference(s: str, table_name:str) -> TableReference:
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
    if main_table == 'self':
        main_table = table_name
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


def read_table_reference(ref:TableReference, index: int, cache: dict)-> Union[str, list[str]]:
    table = ref.table
    df = cache[table]
    conditions = {}
    if len(ref.key) == 0:
        conditions['index'] = index
    for condition, value in ref.key:
        if isinstance(value, TableReference):
            value = read_table_reference(value, index = index, cache = cache)
            if len(value) > 1:
                raise ValueError("multiple values for {key}")
            value = value[0]
        conditions[condition] = value
    
    query_str = ' & '.join([f'{k} == {repr(v)}' for k, v in conditions.items()])
    rows = df.query(query_str)
    result = rows[ref.column].to_list()    
    return result[0]
    

