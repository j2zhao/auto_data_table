import argparse
import os
import table_operations
import file_operations


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="commandline for running things.")
    parser.add_argument("-db","--database", type=str)
    parser.add_argument("-t","--table", type=str )
    parser.add_argument("-op","--operation", type=str)
    parser.add_argument('-r', '--replace', action='store_false')
    parser.add_argument('-a', '--author', type=str, default='command_line')


    args = parser.parse_args()
    db_dir = os.path.join("./example_setup", args.database)
    if args.operation == "database" and args.table is None:
        file_operations.setup_database(db_dir)
    elif args.operation == "table":
        file_operations.setup_table_folder(args.table, db_dir, args.author, args.replace)
    elif args.operation == "update":
        table_operations.update_columns_table(args.table, db_dir,  args.author)
    elif args.operation == "update_rows":
        table_operations.update_rows_table(args.table, db_dir, args.author)
    elif args.operation == "restart":
        table_operations.clean_up_after_restart(db_dir,  args.author)
         