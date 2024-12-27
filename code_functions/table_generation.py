
import re
import os
import pandas as pd

def clean_and_lowercase(text):
    # Use a regular expression to keep only letters
    letters_only = re.sub(r'[^a-zA-Z]', '', text)
    # Convert the result to lowercase
    return letters_only.lower()

def create_paper_table_from_folder(folder_dir, copies, df = None):
    '''Custom'''
    papers = []
    for file in os.listdir(folder_dir):
        if file.endswith('.pdf'):
            name = file.split('.')[0]
            path = os.path.join(folder_dir, file)
            if copies == 1:
                if df == None or not df['paper_name'].str.contains(name).any():
                    papers.append([[name, path]])
            else:
                for i in range(copies):
                    name_ = name + str(i)
                    if df == None or not df['paper_name'].str.contains(name).any():
                        papers.append([[name_, path]])
    
    df_ = pd.DataFrame(papers, columns=["paper_name", "paper_path"])
    if df == None:
        return df_
    else:
        missing_cols = [col for col in df.columns if col not in df_.columns]
        # Add missing columns to df1 with default value ''
        for col in missing_cols:
            df_[col] = ''
        df_1 = pd.concat([df, df_], ignore_index=True)
        #df1_start_idx = len(df)  # The starting index of df1 rows in df_
        #new_rows = list(range(df1_start_idx, df1_start_idx + len(df_)))
        return df_1 #, new_rows 


def create_paper_table_from_folders(folder_name: str, table_name:str, db_dir: str):
    '''Custom'''
    pass

def create_data_table_from_table(columns, previous_table, df = None):
    if df != None:
        return previous_table[columns].copy()
    else:
        merged = previous_table[columns].merge(df, 
                                on=columns, 
                                how='left', 
                                indicator=True)
        
        new_rows_sub = merged.loc[merged['_merge'] == 'left_only', columns]

        # 2) For each new row, fill columns not in 'cols' with empty strings
        missing_cols = [col for col in df.columns if col not in columns]
        new_rows_sub.loc[:, missing_cols] = ""
        
        # 3) Ensure the column order matches df
        new_rows_sub = new_rows_sub[df.columns]
        
        # 4) Append to df and capture the new row indices
        #old_len = len(df)
        updated_df = pd.concat([df, new_rows_sub], ignore_index=True)
        #new_indices = list(range(old_len, old_len + len(new_rows_sub)))
        return updated_df #, new_indices