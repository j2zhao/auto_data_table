'''
Test basic features of database -> more comprehensive testing happens when we actually run the system
'''

import subprocess
import shutil
import os

def copy_files_to_table(base_dir, db_dir, table_name, files):
    org_path = os.path.join(base_dir, table_name)
    new_path = os.path.join(db_dir, table_name)
    new_path = os.path.join(new_path, 'TEMP')
    for file in files:
        org_path_ = os.path.join(org_path, file)
        shutil.copy(org_path_, new_path)

yaml_base_dir = './test_data/test_data_db'
db_dire = './test_database'

def test_single_row():
    # create db
    command = ["python", "execute_operation.py",  "-op", "database", "-db", "test_database"]
    subprocess.run(command)
    raise ValueError()
    # create tables
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)
    command = ["python", "execute_operation.py", "-op", "table", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "table", "-db", "test_database", "-t", "llm_questions"]
    subprocess.run(command)
    # TODO: move yaml to tables
    files = ['fetch_stories.yaml']
    copy_files_to_table(yaml_base_dir, db_dire, "stories", files)
    files = ['fetch_llm_storage.yaml', 'upload_openai.yaml']
    copy_files_to_table(yaml_base_dir, db_dire, "llm_storage", files)
    files = ['fetch_llm_question.yaml', 'question_1.yaml', 'question_2.yaml', 'question_3.yaml']
    copy_files_to_table(yaml_base_dir, db_dire, "llm_questions", files)

    # execute updates
    command = ["python", "execute_operation.py",  "-op", "update", "-db", "test_database", "-t", "stories"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "update", "-db", "test_database", "-t", "llm_storage"]
    subprocess.run(command)
    command = ["python", "execute_operation.py",  "-op", "update", "-db", "test_database", "-t", "llm_questions"]
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
