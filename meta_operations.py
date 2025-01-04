import json
import os
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Optional, Union, Any, Dict
import time
from filelock import FileLock

#TODO: Maybe I don't need the dataclass ! -> 


@dataclass_json
@dataclass
class Log():
    author: str
    start_time:float
    op_time: float
    restarts: list[float] = []
    table_name: str

@dataclass_json
@dataclass
class SetupTableLog(Log): # DONE 
    executed: bool

@dataclass_json
@dataclass
class SetupTableInstanceLog(Log): # DONE
    prev_time_id: Optional[int]

@dataclass_json
@dataclass
class ExecuteTableLog(Log):
    changed_columns: list[str]
    all_columns: list[str]
    time_id = int

@dataclass_json
@dataclass
class FailedExecuteLog(Log): #TODO
    failure: str

@dataclass_json
@dataclass
class DeleteTableLog(Log): # DONE
    time_id: Optional[int]

@dataclass_json
@dataclass
class RestartDatabaseLog(Log): 
    in_progress_tables: list[str]

@dataclass_json
@dataclass
class LogEntry:
    type: str  
    log: Union[SetupTableLog, SetupTableInstanceLog, ExecuteTableLog, FailedExecuteLog, DeleteTableLog, RestartDatabaseLog]


# LOG_MAP = {
#     "SetupTableLog": SetupTableLog,
#     "SetupTableInstanceLog": SetupTableInstanceLog,
#     "ExecuteTableLog": ExecuteTableLog,
#     "FailedExecuteLog": FailedExecuteLog,
#     "DeleteTableLog": FailedExecuteLog,
#     "RestartDatabaseLog": FailedExecuteLog,
# }

@dataclass_json
@dataclass
class TempLog(Log):
    table_name: str
    operation: str
    data: dict[str, Any] = {}

TempLogDict = Dict[str, Dict[str, TempLog]]

ColumnHistoryDict = dict[str, dict[str, list[int]]]
TableHistoryDict = dict[str, list[int]]
# TODO: write column dependencies in the yaml
# TODO: Keep active column updates and active table updates
    
def _serialize_temp_logs(temp_logs: TempLogDict)-> dict:
    serialized_logs = {
        key1: {
            key2: value.to_dict()  # Convert dataclass object to dictionary
            for key2, value in nested.items()
        }
        for key1, nested in temp_logs.items()
    }
    return serialized_logs

def deserialize_temp_logs(serialized_logs:dict) -> TempLogDict:
    deserialized_dict = {
        key1: {
            key2: TempLog.from_dict(value)  # Convert dictionary back to dataclass object
            for key2, value in nested.items()
        }
        for key1, nested in serialized_logs.items()
    }
    return deserialized_dict

class MetaDataStore:        
    def _save_temp_logs(self, temp_logs: TempLogDict) -> None:
        logs = _serialize_temp_logs(temp_logs)
        with open(self.temp_file, 'w') as f:
            json.dump(logs, f, indent=4)

    def _save_column_history(self, columns_history: ColumnHistoryDict) -> None:
        """ Can Optimize This if Needed """
        with open(self.column_history_file, 'w') as f:
            json.dump(columns_history, f, indent=4)
    
    def _save_table_history(self, table_history: TableHistoryDict):
        """ Can Optimize This if Needed """
        with open(self.table_history_file, 'w') as f:
            json.dump(table_history, f, indent=4)

    def _save_logs(self, entry: dict):
        # Convert the log entry to JSON
        # if isinstance(entry, LogEntry):
        #     entry = entry.to_json()
        # Open the file in append mode
        with open(self.log_file, 'a') as file:
            file.write(entry + '\n')
    
    def _get_temp_logs(self) -> TempLogDict: 
        with open(self.temp_file, 'r') as file:
            data = json.load(file)
            temp_logs = deserialize_temp_logs(data) 
            return temp_logs

    def _get_column_history(self) -> ColumnHistoryDict:
        with open(self.column_history_file, 'r') as file:
            columns_history = json.load(file)
            return columns_history
    
    def _get_table_history(self) -> TableHistoryDict:
        with open(self.table_history_file, 'r') as file:
            table_history = json.load(file)
            return table_history

    def __init__(self, db_dir: str) -> None:
        self.db_dir = db_dir
        meta_dir = os.path.join(db_dir, 'metadata')
        self.log_file = os.path.join(meta_dir, 'log.json')
        self.column_history_file = os.path.join(meta_dir, 'columns_history.json')
        self.table_history_file = os.path.join(meta_dir, 'columns_history.json')
        self.temp_file = os.path.join(meta_dir, 'temp_log.json')
        meta_lock = os.path.join(meta_dir, 'meta.lock')
        self.lock = FileLock(meta_lock)
               
        
    def write_to_setup_table_log(self, **kwargs) -> None:
        table_name = kwargs['table_name']
        executed = kwargs['executed']
        with self.lock:
            if executed:
                columns_history = self._get_column_history()
                columns_history[table_name] = {} 
                self._save_column_history(columns_history)
                table_history = self._get_table_history()
                table_history[table_name] = []
                self._save_table_history(table_history)
            kwargs['op_time'] = time.time()
            log = SetupTableLog(**kwargs)
            self.write_to_log(log_entry = LogEntry("SetupTableLog", log), table_name = table_name)


    def write_to_setup_instance_log(self, **kwargs) -> None:
        table_name = kwargs['table_name']
        with self.lock:
            kwargs['op_time'] = time.time()
            log = SetupTableInstanceLog(**kwargs)
            self.write_to_log(log_entry = LogEntry("SetupTableInstanceLog", log), table_name = table_name)

    def write_to_execute_table_log(self, **kwargs):
        table_name = kwargs['table_name']
        changed_columns = kwargs['changed_columns']
        time_id = kwargs['time_id']
        with self.lock:
            table_history = self._get_table_history()
            table_history[table_name].append(time_id)
            self._save_table_history(table_history)
            columns_history = self._get_column_history()
            for column in changed_columns:
                if column not in columns_history[table_name]:
                    columns_history[table_name][column] = []
                columns_history[table_name][column].append(time_id)
                self._save_column_history(columns_history)
            kwargs['op_time'] = time.time()
            log = ExecuteTableLog(**kwargs)
            self.write_to_log(log_entry = LogEntry("ExecuteTableLog", log), table_name = table_name)

    def write_to_delete_table_log(self, **kwargs) -> None:
        table_name = kwargs['table_name']
        time_id = kwargs['time_id']
        with self.lock:     
            if time_id == None:
                table_history = self._get_table_history()
                del table_history[table_name]
                self._save_table_history(table_history)
                column_history = self._get_column_history()
                del column_history[table_name]
                self._save_column_history(table_name)
            else:
                table_history = self._get_table_history()
                column_history = self._get_column_history()
                if time_id in table_history[table_name]:
                    table_history[table_name].remove(time_id)
                    for col in column_history[table_name]:
                        if time_id in column_history[table_name][col]:
                            column_history[table_name][col].remove(time_id)
                self._save_table_history(table_history)
                self._save_column_history(column_history)
            kwargs['op_time'] = time.time()
            log = DeleteTableLog(**kwargs)
            self.write_to_log(log_entry = LogEntry("DeleteTableLog", log), table_name = table_name)
             
            
    def write_to_failed_log(self, **kwargs) -> None:
        table_name = kwargs['table_name']
        with self.lock:
            kwargs['op_time'] = time.time()
            log = FailedExecuteLog(**kwargs)
            self.write_to_log(log_entry = LogEntry("FailedExecuteLog", log), table_name = table_name) 


    def write_to_restart_db_log(self, **kwargs) -> None:
        with self.lock:
            kwargs['op_time'] = time.time()
            kwargs['table_name'] = 'DATABASE'
            log = RestartDatabaseLog(**kwargs)
            self.write_to_log(log_entry = LogEntry("RestartDatabaseLog", log)) 

    def write_to_log(self, log_entry: Union[LogEntry, dict]) -> None:
        with self.lock:
            #table_name = log_entry.log.table_name
            if isinstance(log_entry, LogEntry):
                log_entry = log_entry.to_json()
            author = log_entry['log']['author']
            start_time = log_entry['log']['start_time']
            table_name = log_entry['log']['table_name']
            self.write_to_temp_log(operation = 'write_final_log', table_name = table_name,
                           author = author, start_time = start_time, data = {'log':log_entry}
                           )
            self._save_logs(log_entry)
            temp_logs = self._get_temp_logs()
            del temp_logs[table_name]
            self._save_temp_logs(temp_logs)

    def write_to_temp_log(self, **kwargs):
        if 'table_name' in kwargs['table_name']:
            table_name = kwargs['table_name']
        else:
            table_name = 'DATABASE'
        operation = kwargs['operation']
        with self.lock:
            temp_logs = self._get_temp_logs()
            if table_name not in temp_logs:
                temp_logs[table_name] = {}
            kwargs['op_time'] = time.time()
            log = TempLog(**kwargs)
            temp_logs[table_name][operation] = log
            self._save_temp_logs(temp_logs)
    
    def add_restart_time(self, operation: str, time:float, table_name: str):
        with self.lock:
            temp_logs = self._get_temp_logs()
            if 'restarts' not in temp_logs[table_name][operation].data:
                temp_logs[table_name][operation].data['restarts'] = []
            temp_logs[table_name][operation].data['restarts'].append(time)
            self._save_temp_logs(temp_logs)

    
    def get_all_tables(self) -> list[str]:
        with self.lock:
            tables = list[self._get_table_history().keys()]
        return tables

    def get_last_table_update(self, table_name:str, before_time: Optional[int] = None) -> int:
        with self.lock:
            table_history = self._get_table_history() 
        max_t = 0
        for t in table_history[table_name]:
            if t >= max_t and (before_time == None or t < before_time):
                max_t = t
        return max_t
    
    def get_last_column_update(self, table_name:str, column:str, before_time: Optional[int] = None) -> int:
        with self.lock:
            columns_history = self._get_column_history()
        max_t = 0
        for t in columns_history[table_name][column]:
            if t >= max_t and before_time == None or t < before_time:
                max_t = t
        return max_t
    
    def get_temp_logs(self) -> TempLogDict:
        with self.lock:
            temp_logs = self._get_temp_logs()
            return temp_logs