import argparse
import os
from auto_data_table import table_operations
from auto_data_table import file_operations

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="commandline for running things.")
    parser.add_argument("-db","--database", type=str)
    parser.add_argument("-t","--table", type=str )
    parser.add_argument("-op","--operation", type=str)
    parser.add_argument('-r', '--replace', action='store_true')
    parser.add_argument('-a', '--author', type=str, default='command_line')


    args = parser.parse_args()
    db_dir = os.path.join("./", args.database)
    if args.operation == "database":
        file_operations.setup_database(db_dir, args.replace)
    elif args.operation == "table":
        table_operations.setup_table(args.table, db_dir, args.author, args.replace)
    elif args.operation == "update_columns":
        table_operations.update_columns_table(args.table, db_dir,  args.author)
    elif args.operation == "update_rows":
        table_operations.update_rows_table(args.table, db_dir, args.author)
    elif args.operation == "restart":
        table_operations.clean_up_after_restart(db_dir,  args.author)
         