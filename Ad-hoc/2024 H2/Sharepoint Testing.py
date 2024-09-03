# Databricks notebook source
# pip install Office365-REST-Python-Client

# COMMAND ----------

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

def get_sharepoint_context_using_user():

    # Get sharepoint credentials
    sharepoint_url = 'https://mohitjainloyalytics.sharepoint.com/:f:/s/LoyalyticsAD/EgreoO67225FiCh7B2aneJkBXs05v2Id6LGbOwKq5uskMA?e=w97vDy'

    # Initialize the client credentials
    user_credentials = UserCredential('prem@loyalytics.in', 'a92Ji2!@go@*sh8')

    # create client context object
    ctx = ClientContext(sharepoint_url).with_credentials(user_credentials)

    return ctx

# COMMAND ----------

def create_sharepoint_directory(dir_name: str):
    """
    Creates a folder in the sharepoint directory.
    """
    if dir_name:

        ctx = get_sharepoint_context_using_user()
        
        result = ctx.web.folders.add(f'Shared Documents/Documents/General/{dir_name}').execute_query()

        if result:
            # documents is titled as Shared Documents for relative URL in SP
            relative_url = f'Shared Documents/Documents/General/{dir_name}'
            return relative_url

create_sharepoint_directory('test directory')

# COMMAND ----------

def upload_to_sharepoint(dir_name: str, file_name: str):

    sp_relative_url = create_sharepoint_directory(dir_name)
    ctx = get_sharepoint_context_using_user()

    target_folder = ctx.web.get_folder_by_server_relative_url(sp_relative_url)

    with open(file_name, 'rb') as content_file:
        file_content = content_file.read()
        target_folder.upload_file(file_name, file_content).execute_query()

# COMMAND ----------

import pandas as pd
df = pd.DataFrame({'office_id': [1, 1, 2, 2],
                   'emp_id': [1, 2, 3, 4],
                   'salary': [1000, 2000, 3000, 4000]})

df.to_csv('/dbfs/FileStore/shared_uploads/prem@loyalytics.in/test_data.csv', index = False)

# COMMAND ----------

upload_to_sharepoint('test directory', '/dbfs/FileStore/shared_uploads/prem@loyalytics.in/test_data.csv')
