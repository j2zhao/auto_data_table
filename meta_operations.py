import json
import os
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Optional, Union, Any
from filelock import FileLock

@dataclass_json
@dataclass
class UpdateColumnLog():
    author: str
    table: str
    time: int # this is actually when the CURRENT LOG is executed
    start_time:int
    executed_prompts: list[str]
    changed_columns: list[str]
    all_columns: list[str]

@dataclass_json
@dataclass
class UpdateRowLog():
    author: str
    table: str
    time: int
    start_time:int

@dataclass_json
@dataclass
class SetupTableLog():
    author: str
    table: str
    time: int
    replace: bool

@dataclass_json
@dataclass
class RestartDatabaseLog():
    author: str
    in_progress_tables: list[str]
    start_time: int
    time: Optional[int] 

@dataclass_json
@dataclass
class LogEntry:
    type: str  
    log: Union[UpdateColumnLog, UpdateRowLog, SetupTableLog, RestartDatabaseLog]

@dataclass_json
@dataclass
class TempLog():
    table: str
    author: str
    operation: str
    time: int
    start_time: int
    data: Optional[list]
    

class MetaDataStore:
    def _save_temp_log(self, temp_logs: dict[str, list[TempLog]]) -> None:
        logs = {key: val.to_dict() for key, val in temp_logs.items()}
        with open(self.temp_file, 'w') as f:
            json.dump(logs, f, indent=4)

    def _save_column_history(self, columns_history: dict[str, dict[str, list[int]]]):
        """ Can Optimize This if Needed """
        with open(self.column_history_file, 'w') as f:
            json.dump(columns_history, f, indent=4)

    def _save_log(self, entry: LogEntry):
        # Convert the log entry to JSON
        json_entry = entry.to_json()
        # Open the file in append mode
        with open(self.log_file, 'a') as file:
            file.write(json_entry + '\n')
    
    def _get_temp_log(self):
        with open(self.temp_file, 'r') as file:
            data = json.load(file)
            temp_logs = {key: TempLog.from_dict(val) for key, val in data.items()}
            return temp_logs

    def _get_column_history(self):
        with open(self.column_history_file, 'r') as file:
            columns_history = json.load(file)
            return columns_history


    def __init__(self, db_dir: str) -> None:
        self.db_dir = db_dir
        meta_dir = os.path.join(db_dir, 'metadata')
        self.log_file = os.path.join(meta_dir, 'log.json')
        self.column_history_file = os.path.join(meta_dir, 'columns_history.json')
        self.temp_file = os.path.join(meta_dir, 'temp_log.json')
        self.lock = FileLock(self.log_file)
        
    
    def write_to_setup_table_log(self, author:str, table_name: str, time: int, replace: bool = False) -> None:
        with self.lock:
            log = SetupTableLog(author=author, table=table_name, time=time, replace=replace)
            columns_history = self._get_column_history()
            columns_history[table_name] = {} 
            self._save_column_history(columns_history)
            temp_logs = self._get_temp_log()
            temp_logs[table_name] = {}
            self._save_temp_log(temp_logs)
            self._save_log(LogEntry("SetupTableLog", log))

    def write_to_update_row_log(self, author:str, table_name:str, time:int, start_time:int) -> None:
        with self.lock:
            log = UpdateRowLog(author=author, table=table_name, time=time, start_time=start_time)
            self._save_log(LogEntry("UpdateRowLog", log))

    def write_to_restart_database_log(self, author:str, in_progress_tables:list[str], start_time:int, time:int, clear: bool = False) -> None:
        with self.lock:
            log = RestartDatabaseLog(author=author, in_progress_tables=in_progress_tables, start_time=start_time, time=time)
            self._save_log(LogEntry("RestartDatabaseLog", log))
            if clear:
                temp_logs = self._get_temp_log()
                temp_logs['DATABASE'] = {}
                self._save_temp_log(temp_logs)

    def write_to_update_column_log(self, author: str, table_name: str, time: int, 
                    start_time: int,
                    executed_prompts: list[str], 
                    changed_columns: list[str], 
                    all_columns: list[str],
                    clear: bool = True) -> None:
        with self.lock:
            log = UpdateColumnLog(author=author, table=table_name, time=time, 
                            start_time=start_time,
                            executed_prompts = executed_prompts, 
                            changed_columns=changed_columns, all_columns=all_columns)
            
            self._save_log(LogEntry("UpdateColumnLog", log))
            columns_history = self._get_column_history()
            for column in changed_columns:
                if column not in columns_history[table_name]:
                    columns_history[table_name][column] = []
                columns_history[table_name][column].append(time)
            self._save_column_history(columns_history)
            if clear:
                temp_logs = self._get_temp_log()
                temp_logs[table_name] = {}
                self._save_temp_log()

    def write_to_temp_log(self, author: str, table_name: str, operation: str, time: int, start_time:int, 
                    data: Optional[list] = None) -> int:
        with self.lock:
            temp_logs = self.get_temp_logs()
            temp_logs[table_name][operation] = (TempLog(table=table_name, author=author, operation=operation,
                                                        time= time, start_time=start_time, data= data))
            self._save_temp_log()
    
    def get_all_tables(self) -> list[str]:
        with self.lock:
            columns_history = self._get_column_history() 
        return list(columns_history.keys())

    def get_last_table_update(self, table_name:str, before_time: Optional[int] = None) -> int:
        with self.lock:
            columns_history = self._get_column_history() 
        max_t = 0
        for col in columns_history[table_name]:
            for t in columns_history[table_name][col]:
                if t >= max_t and before_time == None or t < before_time:
                    max_t = t
        return max_t
    
    def get_last_column_update(self, table_name, column, before_time = None) -> int:
        with self.lock:
            columns_history = self._get_column_history()
        max_t = 0
        for t in columns_history[table_name][column]:
            if t >= max_t and before_time == None or t < before_time:
                max_t = t
        return max_t
    
    def get_temp_logs(self, table_name = None, 
                      operation = None) -> Any:
        with self.lock:
            temp_logs = self.get_temp_logs()
        
        if table_name == None:
            return temp_logs
        if not operation:
            return temp_logs[table_name]
        else:
            return temp_logs[table_name][operation]