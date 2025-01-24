import argparse
import os
from auto_data_table import table_operations
from auto_data_table import file_operations


#TODO: OPERATIONS
# read current active log
# create database
# create table
# create table instance
# delete table
# delete table instance
# execute table
# restart database


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="commandline for running things.")
    parser.add_argument("-db","--database", type=str)
    parser.add_argument("-op","--operation", type=str)
    parser.add_argument('-a', '--author', type=str, default='command_line')

    parser.add_argument("-t","--table", type=str)
    parser.add_argument('-r', '--replace', action='store_true')
    parser.add_argument('-id', '--time_id', type=int)
    parser.add_argument('-p', '--prompts', nargs='*', type=str, default=[])
    parser.add_argument('-gp', '--gen_prompt', type=str, default = '')

    args = parser.parse_args()
    db_dir = os.path.join("./", args.database)
    if args.operation == "database":
        file_operations.setup_database(db_dir, args.replace)
    elif args.operation == "table":
        table_operations.setup_table(args.table, db_dir, args.author, args.replace)
    elif args.operation == "table_instance":
        table_operations.setup_table_instance(args.table, db_dir,  args.author, args.time_id, args.prompts, args.gen_prompt)
    elif args.operation == "delete":
        table_operations.delete_table(args.table, db_dir, args.author, args.time_id)
    elif args.operation == "execute":
        table_operations.execute_table(args.table, db_dir, args.author)
    elif args.operation == "restart":
        table_operations.clean_up_after_restart(db_dir,  args.author)
         