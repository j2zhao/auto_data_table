import json
import os
from dataclasses import dataclass, field, asdict
from dataclasses_json import dataclass_json
from typing import Optional, Union, Any, Dict
import time
from filelock import FileLock
import uuid
import pprint

#TODO: there is an edge case where i might write to log twice -> Okay for now


@dataclass_json
@dataclass
class ProcessLog():
    process_id: str
    author:str
    start_time:float
    log_time: float
    table_name: str
    instance_id: str
    restarts: list[tuple[str, float]]
    operation: str
    complete_steps: list[str]
    step_times: list[float]
    data: dict[str, Any]
    success: Optional[bool]

# @dataclass_json
# @dataclass
# class SetupTableLog(ProcessLog): # DONE 
#     executed: bool
#     allow_multiple: bool

# @dataclass_json
# @dataclass
# class SetupTableInstanceLog(ProcessLog): # DONE
#     prev_instance_id: str
#     prompts: list[str]

# @dataclass_json
# @dataclass
# class ExecuteTableLog(ProcessLog):
#     instance_id: str
#     materialization_time: float
#     changed_columns: list[str]
#     all_columns: list[str]

# @dataclass_json
# @dataclass
# class FailedExecuteLog(ProcessLog): #TODO
#     failure: str

# @dataclass_json
# @dataclass
# class DeleteTableLog(ProcessLog): # DONE
#     instance_id: str

# @dataclass_json
# @dataclass
# class RestartDatabaseLog(ProcessLog): 
#     finished_processes: list[str] # list of process ids?

# @dataclass_json
# @dataclass
# class ActiveProcessLog(ProcessLog):
#     operation: str
#     complete_steps: list[str]
#     data: dict[str, Any] # {sub_operation: {data_name: data}}


ColumnHistoryDict = dict[str, dict[str, dict[str, dict[str, float]]]]
TableHistoryDict = dict[str, dict[str, float]]
TableMultipleDict = dict[str, bool]
ActiveProcessDict = Dict[str, ProcessLog]

def _serialize_active_log(temp_logs: ActiveProcessDict) -> dict:
    serialized_logs = {
            key: value.to_dict()  # Convert dataclass object to dictionary
            for key, value in temp_logs.items()
        }
    return serialized_logs

def _deserialize_active_log(serialized_logs: dict) -> ActiveProcessDict:
    deserialized_dict = {
                key: ProcessLog.from_dict(value)  # Convert dictionary back to dataclass object
                for key, value in serialized_logs.items()
    }
    return deserialized_dict


class MetaDataStore:
    # on failure: do nothing...on restarts -> have option to revert temp tables
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
    
    def _save_table_multiple(self, table_multiples: TableMultipleDict):
        with open(self.table_multiple_file, 'w') as f:
            json.dump(table_multiples, f, indent=4)  

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
    
    def _get_table_multiple(self) -> TableMultipleDict:
        with open(self.table_multiple_file, 'r') as file:
            table_multiples = json.load(file)
            return table_multiples
    
    def _write_to_log(self, log_entry: ProcessLog) -> None:
        process_id = log_entry.process_id
        self._update_process_internal(process_id, 'write_log')
        log_entry.log_time = time.time()
        log_entry = asdict(log_entry)
        with open(self.log_file, 'a') as file:
            file.write(json.dumps(log_entry) + '\n')
        self._delete_process_internal(process_id)
        

    def __init__(self, db_dir: str) -> None:
        self.db_dir = db_dir
        meta_dir = os.path.join(db_dir, 'metadata')
        self.log_file = os.path.join(meta_dir, 'log.txt')
        self.column_history_file = os.path.join(meta_dir, 'columns_history.json')
        self.table_history_file = os.path.join(meta_dir, 'tables_history.json')
        self.table_multiple_file = os.path.join(meta_dir, 'tables_multiple.json')
        self.active_file = os.path.join(meta_dir, 'active_log.json')
        meta_lock = os.path.join(meta_dir, 'LOG.lock') 
        self.lock = FileLock(meta_lock)

    def _setup_table_operation(self, log: ProcessLog) -> None:
        table_name = log.table_name
        columns_history = self._get_column_history()
        columns_history[table_name] = {} 
        self._save_column_history(columns_history)
        table_history = self._get_table_history()
        table_history[table_name] = {}
        self._save_table_history(table_history)
        table_multiples = self._get_table_multiple()
        table_multiples[table_name] = log.data['allow_multiple']
        self._save_table_multiple(table_multiples)

    def _setup_instance_operation(self, log: ProcessLog) -> None:
        "Nothing happens -> temp instance shouldn't impact metadata"
        pass

    def _delete_table_operation(self, log: ProcessLog) -> None:
        table_name = log.table_name
        table_history = self._get_table_history()
        if table_name in table_history:
            del table_history[table_name]
        self._save_table_history(table_history)
        column_history = self._get_column_history()
        if table_name in column_history:
            del column_history[table_name]
        self._save_column_history(table_name)
        table_multiples = self._get_table_multiple()
        if table_name in table_multiples:
            del table_multiples[table_name]
        self._save_table_multiple(table_multiples)

    def _delete_instance_operation(self, log: ProcessLog) -> None:
        table_history = self._get_table_history()
        column_history = self._get_column_history()
        instance_id = log.data['instance_id']
        table_name = log.table_name
        if instance_id in table_history[table_name]:
            del table_history[table_name][instance_id]
        if instance_id in column_history[table_name]:
            del column_history[table_name][instance_id]
        self._save_table_history(table_history)
        self._save_column_history(column_history)

    def _execute_operation(self, log: ProcessLog) -> None:
        table_name = log.table_name
        table_time = log.data['start_time']
        instance_id = log.data['perm_instance_id']
        changed_columns = log.data['to_change_columns']
        gen_columns = log.data['gen_columns']
        all_columns = log.data['all_columns']
        prev_instance_id = log.data['origin']
        table_history = self._get_table_history()
        table_history[table_name][instance_id] = table_time
        self._save_table_history(table_history)
        columns_history = self._get_column_history()
        columns_history[table_name][instance_id] = {}
        for column in all_columns:
            if column in changed_columns or column in gen_columns:
                columns_history[table_name][instance_id][column] = table_time
            else:
                columns_history[table_name][instance_id][column] = columns_history[table_name][prev_instance_id][column]
        self._save_column_history(columns_history)

    def _restart_operation(self, log: ProcessLog) -> None:
        "Nothing Happens For Now"
        pass

    def write_to_log(self, process_id, success = True):
        with self.lock:
            log = self._get_active_log()[process_id]
            log.success = success
            if log.operation == 'setup_table': 
                self._setup_table_operation(log)
            elif log.operation == 'setup_table_instance': 
                self._setup_instance_operation(log)
            elif log.operation == 'delete_table':
                self._delete_table_operation(log)
            elif log.operation == 'delete_table_instance':
                self._delete_instance_operation(log)
            elif log.operation == 'restart_database':
                self._restart_operation(log)
            elif log.operation == 'execute_table' and success:
                self._execute_operation(log)
            else:
                raise NotImplementedError()
            self._write_to_log(log)
    
    def start_new_process(self, author:str, operation: str, table_name:str, instance_id:str = '', start_time: Optional[float] = None, data:dict[str, Any] = {}) -> float:
        with self.lock:
            process_id = str(uuid.uuid4())
            active_processes = self._get_active_log()
            if not start_time:
                start_time = time.time()
            restarts = []
            active_processes[process_id] = ProcessLog(process_id, author, start_time, start_time, table_name, instance_id, restarts, operation, [], [], data, None)
            self._save_active_log(active_processes)
            return process_id
    
    def update_process_data(self, process_id:str, data:dict):
        with self.lock:
            active_processes = self._get_active_log()
            active_processes[process_id].log_time = time.time()
            active_processes[process_id].data.update(data)
            self._save_active_log(active_processes)
    
    def _update_process_internal(self,  process_id:str, step: str):
        active_processes = self._get_active_log()
        active_processes[process_id].complete_steps.append(step)
        active_processes[process_id].step_times.append(time.time())
        active_processes[process_id].log_time = time.time()
        self._save_active_log(active_processes)
    
    def update_process_step(self, process_id:str, step: str):
        with self.lock:
            self._update_process_internal(process_id, step)
    
    def update_process_restart(self, author:str, process_id:str) -> ProcessLog:
        with self.lock:
            restart_time = time.time()
            active_processes = self._get_active_log()
            active_processes[process_id].restarts.append((author, restart_time))
            active_processes[process_id].log_time = time.time()
            self._save_active_log(active_processes)
            return active_processes[process_id]
    
    def _delete_process_internal(self, process_id: str):
        active_processes = self._get_active_log()
        del active_processes[process_id]
        self._save_active_log(active_processes)
        
    def get_all_tables(self) -> list[str]:
        with self.lock:
            tables = list[self._get_table_history().keys()]
        return tables

    def _get_multiple_internal(self, table_name):
        allow_multiples = self._get_table_multiple()
        return allow_multiples[table_name]
    
    def get_table_multiple(self, table_name:str):
        with self.lock:
            return self._get_multiple_internal(table_name)

    def get_table_version_update(self, instance_id:str, table_name:str,
                                 before_time: Optional[int] = None):
        with self.lock:
            table_history = self._get_table_history() 
            vtime =  table_history[table_name][instance_id]
            if before_time == None or vtime < before_time:
                return vtime
            else:
                return 0

    def get_column_version_update(self, column_name, instance_id:str, table_name:str,
                                  before_time: Optional[int] = None):
        with self.lock:
            column_history = self._get_column_history() 
            vtime = column_history[table_name][instance_id][column_name]
            if vtime < before_time:
                return vtime
            else:
                return 0
   
    def get_last_table_update(self, table_name:str, before_time: Optional[int] = None) -> Union[int, str]:
        '''
        Returns 0 when we didn't find any tables that meet conditions.
        Return -1 when the table was last updated after before_times and it can only have one active version.
        '''
        with self.lock:
            table_history = self._get_table_history() 
            max_t = 0
            max_id = 0
            for instance_id, t in table_history[table_name].items():
                if t > max_t and (before_time == None or t < before_time):
                    max_t = t
                    max_id = instance_id
            return max_t, max_id
    
    def get_last_column_update(self, table_name:str, column:str, before_time: Optional[int] = None) -> int:
        '''
        Returns 0 when we didn't find any tables that meet conditions.
        Return -1 when the table was last updated after before_times and it can only have one active version.
        '''
        with self.lock:
            columns_history = self._get_column_history()
            max_t = 0
            max_id = 0
            for instance_id in columns_history[table_name]:
                if column in columns_history[table_name][instance_id]:
                    t = columns_history[table_name][instance_id][column]
                    if t >= max_t and (before_time == None or t < before_time):
                        max_t = t
                        max_id = instance_id
            return max_t, max_id


    def teminate_previous_restarts(self):
        with self.lock:
            active_logs = self._get_active_log()
            ids = []
            for process_id, process in active_logs.items():
                if process.operation == 'restart_database':
                    ids.append(process_id)
            for id in ids:
                active_logs[id].success = False
                self._write_to_log(active_logs[id])

    def get_process_ids(self) -> list[tuple[str, str]]:
        with self.lock:
            active_logs = self._get_active_log()
            ids = []
            for id, process in active_logs.items():
                ids.append((id, process.operation))
            return ids
        
    def print_active_logs(self) -> None:
        with self.lock:
            active_logs = self._get_active_log()
            pprint(active_logs)
        
    def write_to_log_after_restart(self):
        with self.lock:
            active_logs = self._get_active_log()
            for process_id, process in active_logs.items():
                if 'write_log' in process.complete_steps:
                    self._write_to_log(process)

    