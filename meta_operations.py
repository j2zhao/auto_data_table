import json
import os
from dataclasses import dataclass, field, asdict
from dataclasses_json import dataclass_json
from typing import Optional, Union, Any, Dict
import time
from filelock import FileLock

#TODO: Maybe I don't need the dataclass ! -> 


@dataclass_json
@dataclass
class ProcessLog():
    process_id: str
    start_time:float
    log_time: float
    table_name: str
    restarts: list[tuple[str, float]]

@dataclass_json
@dataclass
class SetupTableLog(ProcessLog): # DONE 
    executed: bool
    allow_multiple: bool

@dataclass_json
@dataclass
class SetupTableInstanceLog(ProcessLog): # DONE
    prev_instance_id: str
    prompts: list[str]

@dataclass_json
@dataclass
class ExecuteTableLog(ProcessLog):
    instance_id: str
    materialization_time: float
    changed_columns: list[str]
    all_columns: list[str]

@dataclass_json
@dataclass
class FailedExecuteLog(ProcessLog): #TODO
    failure: str

@dataclass_json
@dataclass
class DeleteTableLog(ProcessLog): # DONE
    instance_id: str

@dataclass_json
@dataclass
class RestartDatabaseLog(ProcessLog): 
    finished_processes: list[str] # list of process ids?

@dataclass_json
@dataclass
class ActiveProcessLog():
    operation: str
    logs: dict[str, Any] # list of operation names


ColumnHistoryDict = dict[str, dict[str, dict[str, float]]]
TableHistoryDict = dict[str, dict[str, float]]
ActiveProcessDict = Dict[str, ActiveProcessLog]

def _serialize_active_log(temp_logs: ActiveProcessDict) -> dict:
    serialized_logs = {
            key: value.to_dict()  # Convert dataclass object to dictionary
            for key, value in temp_logs.items()
        }
    return serialized_logs

def _deserialize_active_log(serialized_logs: dict) -> ActiveProcessDict:
    deserialized_dict = {
                key: ActiveProcessLog.from_dict(value)  # Convert dictionary back to dataclass object
                for key, value in serialized_logs.items()
    }
    return deserialized_dict


class MetaDataStore:        
    def _save_active_log(self, temp_logs: ActiveProcessDict) -> None:
        logs = _serialize_active_log(temp_logs)
        with open(self.active_file, 'w') as f:
            json.dump(logs, f, indent=4)

    def _save_column_history(self, columns_history: ColumnHistoryDict) -> None:
        with open(self.column_history_file, 'w') as f:
            json.dump(columns_history, f, indent=4)
    
    def _save_table_history(self, table_history: TableHistoryDict):
        with open(self.table_history_file, 'w') as f:
            json.dump(table_history, f, indent=4)

    def _save_logs(self, entry: dict):
        with open(self.log_file, 'a') as file:
            file.write(json.dumps(entry) + '\n')
    
    def _get_active_log(self) -> ActiveProcessDict: 
        with open(self.active_file, 'r') as file:
            data = json.load(file)
            temp_logs = _deserialize_active_log(data) 
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
        self.log_file = os.path.join(meta_dir, 'log.txt')
        self.column_history_file = os.path.join(meta_dir, 'columns_history.json')
        self.table_history_file = os.path.join(meta_dir, 'tables_history.json')
        self.active_file = os.path.join(meta_dir, 'active_log.json')
        meta_lock = os.path.join(meta_dir, 'LOG.lock') 
        self.lock = FileLock(meta_lock)
               
        
    def write_to_setup_table_log(self, author:str, start_time:float, table_name:str,
                                   executed:bool, restarts: list[float] = []) -> None:
        with self.lock:
            if executed:
                columns_history = self._get_column_history()
                columns_history[table_name] = {} 
                self._save_column_history(columns_history)
                table_history = self._get_table_history()
                table_history[table_name] = {}
                self._save_table_history(table_history)
            op_time = time.time()
            log = SetupTableLog(author, start_time, op_time, table_name, restarts, executed)
            self.write_to_log(log_entry = LogEntry("SetupTableLog", log))


    def write_to_setup_instance_log(self, author: str, start_time:float, table_name:str, 
                                  prev_name_id: Optional[str], 
                                  prompts: list[str],
                                  restarts: list[float] = [] ) -> None:
        with self.lock:
            op_time = time.time()
            log = SetupTableInstanceLog(author, start_time, op_time, table_name, restarts, prev_name_id, prompts)
            self.write_to_log(log_entry = LogEntry("SetupTableInstanceLog", log))

    def write_to_execute_table_log(self,author: str, start_time:float, table_name:str, table_id: str, 
                                   table_time:float,
                                   changed_columns: list[str], all_columns: list[str], 
                                   restarts: list[float] = []):
        with self.lock:
            table_history = self._get_table_history()
            table_history[table_name][table_id] = table_time
            self._save_table_history(table_history)
            columns_history = self._get_column_history()
            for column in changed_columns:
                if column not in columns_history[table_name]:
                    columns_history[table_name][column] = {}
                columns_history[table_name][column][table_id] = table_time
            self._save_column_history(columns_history)
            op_time = time.time()
            log = ExecuteTableLog(author,start_time, op_time, table_name, restarts, table_time,
                                  table_id,
                                  changed_columns, all_columns)
            self.write_to_log(log_entry = LogEntry("ExecuteTableLog", log))

    def write_to_delete_table_log(self,author: str, start_time:float, table_name:str, 
                                  table_id:str = None,  restarts: list[float] = []) -> None:
        with self.lock:     
            if table_id == None:
                table_history = self._get_table_history()
                del table_history[table_name]
                self._save_table_history(table_history)
                column_history = self._get_column_history()
                del column_history[table_name]
                self._save_column_history(table_name)
            else:
                table_history = self._get_table_history()
                column_history = self._get_column_history()
                if table_id in table_history[table_name]:
                    del table_history[table_name][table_id]
                for col in column_history[table_name]:
                    if table_id in column_history[table_name][col]:
                        del column_history[table_name][col][table_id]
                self._save_table_history(table_history)
                self._save_column_history(column_history)
            op_time = time.time()
            log = DeleteTableLog(author, start_time, op_time, table_name, restarts, table_id)
            self.write_to_log(log_entry = LogEntry("DeleteTableLog", log))
             
            
    def write_to_failed_log(self, author: str, start_time:float, table_name:str, 
                            failure: str,  restarts: list[float] = []) -> None:
        with self.lock:
            op_time = time.time()
            log = FailedExecuteLog(author, start_time, op_time, table_name, restarts, failure)
            self.write_to_log(log_entry = LogEntry("FailedExecuteLog", log)) 


    def write_to_restart_db_log(self, author: str, start_time:float, in_progress_tables:list[str]
                                ) -> None:
        with self.lock:
            op_time = time.time()
            table_name = 'DATABASE'
            restarts = []
            log = RestartDatabaseLog(author, start_time, op_time, table_name, restarts, in_progress_tables)
            self.write_to_log(log_entry = LogEntry("RestartDatabaseLog", log)) 

    def write_to_log(self, log_entry: Union[LogEntry, dict]) -> None:
        with self.lock:
            if isinstance(log_entry, LogEntry):
                log_entry = asdict(log_entry)
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

    def start_new_process(self, process_id:str, operation: str, table_name:str) -> float:
        with self.lock:
            active_processes = self._get_processes()
            start_time = time.time()
            restarts = []
            active_processes[process_id] = ActiveProcessLog(process_id, start_time, start_time, table_name, restarts, operation, {})
            self._save_active_log(active_processes)
    # def write_to_temp_log(self, process_id:str, author: str, start_time:float,
    #                       operation:str, data: dict[str, Any] = {}, table_name = 'DATABASE'):
    #     with self.lock:
    #         temp_logs = self._get_temp_logs()
    #         if table_name not in temp_logs:
    #             temp_logs[table_name] = {}
    #         op_time = time.time()
    #         restarts= []
    #         log = TempLog(author, start_time, op_time, table_name, restarts, operation, data)
    #         if process_id not in temp_logs[table_name]:
    #             temp_logs[table_name][process_id] = {}
    #         temp_logs[table_name][process_id][operation] = log
    #         self._save_temp_logs(temp_logs)
        return start_time

    
    def get_all_tables(self) -> list[str]:
        with self.lock:
            tables = list[self._get_table_history().keys()]
        return tables

    def get_last_table_update(self, table_name:str, before_time: Optional[int] = None) -> int:
        with self.lock:
            table_history = self._get_table_history() 
        max_t = 0
        max_id = None
        for table_id, t in table_history[table_name].items():
            if t >= max_t and (before_time == None or t < before_time):
                max_t = t
                max_id = table_id
        return max_id
    
    def get_last_column_update(self, table_name:str, column:str, before_time: Optional[int] = None) -> int:
        with self.lock:
            columns_history = self._get_column_history()
        max_t = 0
        max_id = None
        for table_id, t in columns_history[table_name][column].items():
            if t >= max_t and before_time == None or t < before_time:
                max_t = t
                max_id = table_id
        return max_id
    
    def get_active_log(self) -> ActiveProcessDict:
        with self.lock:
            temp_logs = self._get()
            return temp_logs