import os
import json
import streamlit as st
from beepbeep_txflowutils.utils.utils import Utilities
from source import source
from beepbeep_txflowutils.secrets.config_setting import config_secret
if st.checkbox("Bigquery Connection"):
   st.subheader("Input information to connect into Bigquery to query: \n INFORMATION_SCHEMA.(PARAMETERS, ROUTINES, ROUTINE_OPTIONS and QUERY VIEW TABLES)")
   PROJECT_ID = st.text_input("Project ID", value=config_secret['PROJECT_ID'])
   DATASET_ID = st.text_input("Dataset ID", value=config_secret['DATASET_ID'])

   service_account_to_str= json.dumps(config_secret['CRED_BASE_TEST'])
   service_account_res = st.text_input("Json service account: e.g: {...}", value=service_account_to_str)
   service_account_to_json = json.loads(service_account_res)

   st.subheader("Markdown information to set template")
   # TODO: convert into snake format folder_render_name and file_render_name if it's neccesary
   folder_render_name = st.text_input("Folder name to send Markdown file rendered", value='folder_name')
   file_render_name = st.text_input('Markdown file name to render the content', value='file_name')
   dtf_main_h1 = st.text_input("Main and global title of the template", value='Data Transformation Functions')
   dtf_main_description = st.text_area("Main and global description of the template", value='These functions form the core of the framework and aim to automate and simplify common data transformation patterns.')
   #dtf_subheader_h2 = st.text_input("Subheader title", value='Subheader title')
   #dtf_subheader_h2_description = st.text_area("subheader description", value='Subheader description')

   st.subheader("GitHub username and local folder to push into GitHub is temporarily deactivated!!!")
   GITHUB_REPO_USERNAME = st.text_input("GitHub repository username to send Markdon template file", value=config_secret['GITHUB_REPO_USERNAME'])
   GITHUB_LOCAL_REMOTE_NAME = st.text_input("Local folder to push into GitHub", value=config_secret['GITHUB_LOCAL_REMOTE_NAME'])
   
   if st.button("Send"):
       #st.subheader("Data result:")
       config_secret['PROJECT_ID'] = PROJECT_ID
       config_secret['DATASET_ID'] = DATASET_ID
       config_secret['CRED_BASE_TEST'] = service_account_to_json
       config_secret['folder_render_name'] = folder_render_name
       config_secret['file_render_name'] = file_render_name
       config_secret['dtf_main_h1'] = dtf_main_h1
       config_secret['dtf_main_description'] = dtf_main_description
       #config_secret['dtf_subheader_h2'] = dtf_subheader_h2
       #config_secret['dtf_subheader_h2_description'] = dtf_subheader_h2_description
       config_secret['GITHUB_REPO_USERNAME'] = GITHUB_REPO_USERNAME
       config_secret['GITHUB_LOCAL_REMOTE_NAME'] = GITHUB_LOCAL_REMOTE_NAME

       res = source()

       markdown_dir_path = Utilities()._markdown_dir_path()
       folder_render_name_path = os.path.join(markdown_dir_path, folder_render_name)
       file_render_name_path = os.path.join(folder_render_name_path, f"{file_render_name}.md")

       if os.path.exists(file_render_name_path):
           with open(file_render_name_path, 'r') as r:
               md_content = r.read()
               st.markdown(md_content)
