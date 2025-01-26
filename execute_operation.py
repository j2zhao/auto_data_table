import argparse
import os
from auto_data_table import table_operations
from auto_data_table import file_operations
from auto_data_table.meta_operations import MetaDataStore

#TODO: OPERATIONS
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
    # operator specific operations
    parser.add_argument("-t","--table", type=str)
    parser.add_argument('-r', '--replace', action='store_true')
    parser.add_argument('-m', '--multiple', action='store_true')
    parser.add_argument('-pid', '--prev_id', type=str, default = '')
    parser.add_argument('-p', '--prompts', nargs='*', type=str, default=[])
    parser.add_argument('-gp', '--gen_prompt', type=str, default = '')
    parser.add_argument('-id', '--instance_id', type=str, default = 'TEMP')
    parser.add_argument('-ex', '--excluded', nargs='*', type=str, default=[])
    # parser.add_argument('-m', '--multiple', action='store_true')

    args = parser.parse_args()
    db_dir = os.path.join("./", args.database)
    if args.operation == 'logs':
        db_metadata = MetaDataStore(db_dir)
        db_metadata.print_active_logs()
    elif args.operation == "database":
        file_operations.setup_database(db_dir, args.replace)
    elif args.operation == "table":
        table_operations.setup_table(args.table, db_dir, args.author, args.multiple)
    elif args.operation == "table_instance":
        table_operations.setup_table_instance(args.instance_id, args.table, db_dir, args.author,args.prev_id, args.prompts, args.gen_prompt)
    elif args.operation == "delete_table":
        table_operations.delete_table(args.table,db_dir, args.author)
    elif args.operation == "delete_instance":
        table_operations.delete_table_instance(args.instance_id, args.table, db_dir)
    elif args.operation == "execute":
        table_operations.execute_table(args.table, db_dir, args.author, args.instance_id)
    elif args.operation == "restart":
        table_operations.restart_database(args.author, db_dir, excluded_processes=args.excluded)


    # elif args.operation == "restart":
    #     table_operations.clean_up_after_restart(db_dir,  args.author)
         