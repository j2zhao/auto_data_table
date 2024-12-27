import openai
import pandas as pd
from llm_functions.open_ai_thread import  add_open_ai_secret
from tqdm import tqdm


def delete_files(client):
    files = list(client.files.list())
    vector_stores = list(client.beta.vector_stores.list())
    my_assistants = list(client.beta.assistants.list())
    for store in tqdm(vector_stores):
        try:
          client.beta.vector_stores.delete(
            vector_store_id=store.id
          )
        except:
            pass
    for f in tqdm(files):
        try:
          client.files.delete(
            file_id=f.id
          )
        except:
          pass
    
    for assistant in tqdm(my_assistants):
        try:
            client.beta.assistants.delete(assistant.id)
        except:
            pass
    
    print(client.beta.vector_stores.list())
    print(client.files.list())
    print(client.beta.assistants.list())


def upload_file_from_table(file_path, key_file):
    with open(key_file, 'r') as f:
        secret = f.read()
        add_open_ai_secret(secret)
    
    client = openai.OpenAI()
     
        
    file = client.files.create(
                      file=open(file_path, "rb"), purpose="assistants"
                    )
    return file.id