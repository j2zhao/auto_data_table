'''
Test basic features of database -> more comprehensive testing happens when we actually run the system
'''

import subprocess
import shutil
import os

def copy_files_to_table(base_dir, db_dir, table_name):
    org_path = os.path.join(base_dir, table_name)
    new_path = os.path.join(db_dir, table_name)
    #new_path = os.path.join(new_path, 'TEMP')
    new_path = os.path.join(new_path, 'prompts')
    for file in os.listdir(org_path):
        if file.endswith('.yaml'):
            org_path_ = os.path.join(org_path, file)
            new_path_ = os.path.join(new_path, file)
            if os.path.exists(new_path_):
                os.remove(new_path_)
            shutil.copy2(org_path_, new_path_)

yaml_base_dir = './test_data/test_data_db'
db_dire = './test_database'

def test_single_row():
    # create db
    command = ["python", "execute_operation.py",  "-op", "database", "-db", "test_database", "-r"]
    subprocess.run(command)
    # create tables
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)
    #command = ["python", "execute_operation.py",  "-op", "table", "-db", "test_database", "-t", "llm_questions"]
    #subprocess.run(command)
    #command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "stories"]
    #subprocess.run(command)
    #raise ValueError()
    #files = ['fetch_stories.yaml', 'fetch_stories_5.yaml']
    copy_files_to_table(yaml_base_dir, db_dire, "stories")
    #files = ['fetch_llm_storage.yaml', 'remove_ll.yaml']
    copy_files_to_table(yaml_base_dir, db_dire, "llm_storage")
    #raise ValueError()
    
    #raise ValueError()
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "stories", 
               "-p", "fetch_stories", "-gp", "fetch_stories"]
    subprocess.run(command)
    #raise ValueError()
    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
               "-p", "fetch_llm_storage", "upload_openai", "-gp", "fetch_llm_storage"]
    subprocess.run(command)
    #raise ValueError()
    # execute updates
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)
    #raise ValueError()
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)

    command = ["python", "execute_operation.py", "-op", "table_instance", "-db", "test_database", "-t", "llm_storage", 
               "-p", "remove_llm_storage", "-gp", "remove_llm_storage"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "execute", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)

def test_five_rows():
    pass

def test_column_update():
    pass

def test_row_update():
    pass

def test_column_restart():
    # USE timeout test
    pass

def test_row_restart():
    # USE timeout test
    pass

def cleanup_folder():
    shutil.rmtree(yaml_base_dir)

if __name__ == '__main__':
    test_single_row()
    #shutil.rmtree(yaml_base_dir)