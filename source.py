from beepbeep_txflowutils.secrets.config_setting import config_secret
from parameters import BigqueryParameters
from routines import BigqueryRoutines
from routine_options import BigqueryRoutineOptions
from queryViews import BigqueryViews
from md_render_prototype import MarkdownRender


def source():
    """
    Source function creates and executes instance of each class we have into it.
    TODO: Exceptions and Exceptions and Errors
    """
    bigquery_parameters_obj = BigqueryParameters(
                project_id=config_secret['PROJECT_ID'], 
                dataset_id=config_secret['DATASET_ID'], 
                #path="personal_bq.json", # "personal_bq.json"
                json_credential=config_secret['CRED_BASE_TEST'], # json_credential
                github_repo_is_org=True,
                github_repo_username=config_secret['GITHUB_REPO_USERNAME'],
                github_local_remote_name=config_secret['GITHUB_LOCAL_REMOTE_NAME'],
                github_update_repo=True,
                github_commit_comment="Refactoring parameters"
            )
    bigquery_parameters_obj.info_schema()


    bigquery_routines_obj = BigqueryRoutines(
                project_id=config_secret['PROJECT_ID'], 
                dataset_id=config_secret['DATASET_ID'], 
                #path="personal_bq.json", # "personal_bq.json"
                json_credential=config_secret['CRED_BASE_TEST'], # json_credential
                github_repo_is_org=True,
                github_repo_username=config_secret['GITHUB_REPO_USERNAME'],
                github_local_remote_name=config_secret['GITHUB_LOCAL_REMOTE_NAME'],
                github_update_repo=True,
                github_commit_comment="Refactoring Routines"
            )
    bigquery_routines_obj.info_schema()


    bigquery_routine_options_obj = BigqueryRoutineOptions(
                project_id=config_secret['PROJECT_ID'], 
                dataset_id=config_secret['DATASET_ID'], 
                #path="personal_bq.json", # "personal_bq.json"
                json_credential=config_secret['CRED_BASE_TEST'], # json_credential
                github_repo_is_org=True,
                github_repo_username=config_secret['GITHUB_REPO_USERNAME'],
                github_local_remote_name=config_secret['GITHUB_LOCAL_REMOTE_NAME'],
                github_update_repo=True,
                github_commit_comment="Refactoring Routine Options"
            )
    bigquery_routine_options_obj.info_schema()


    bigquery_views_obj = BigqueryViews(
                project_id=config_secret['PROJECT_ID'], 
                dataset_id=config_secret['DATASET_ID'], 
                #path="personal_bq.json", # "personal_bq.json"
                json_credential=config_secret['CRED_BASE_TEST'], # json_credential
                github_repo_is_org=True,
                github_repo_username=config_secret['GITHUB_REPO_USERNAME'],
                github_local_remote_name=config_secret['GITHUB_LOCAL_REMOTE_NAME'],
                github_update_repo=True,
                github_commit_comment="Refactoring Query Views"
            )
    bigquery_views_obj.info_schema()


    md_render_obj = MarkdownRender()
    md_render_obj.md_bucket_dir(
        folder_render_name=config_secret['folder_render_name'], 
        file_render_name=config_secret['file_render_name']
    )
    res = md_render_obj.data_transformation_function_template(
        dtf_main_h1=config_secret['dtf_main_h1'],
        dtf_main_description=config_secret['dtf_main_description'],
        dtf_subheader_h2=config_secret['dtf_subheader_h2'],
        dtf_subheader_h2_description=config_secret['dtf_subheader_h2_description']
    )

    return res

