import os
from beepbeep_txflowutils.secrets.config_setting import config_secret
from beepbeep_txflowutils.auth.service_account import BigquerySource
from beepbeep_txflowutils.auth.info_schema import InfoSchema
from beepbeep_txflowutils.github_source.github_source import GitHubSource
from beepbeep_txflowutils.utils.utils import Utilities


class BigqueryParameters(BigquerySource, InfoSchema, GitHubSource, Utilities):
    # Constructor
    def __init__(self, 
                    project_id, 
                    dataset_id,  
                    path=None, 
                    json_credential=None,
                    github_repo_is_org = False,
                    github_repo_username = None,
                    github_local_remote_name = None,
                    github_update_repo = False,
                    github_commit_comment=None,
                    github_repo_branch: str = "main"
                ):
        self.root_path = os.path.join(InfoSchema._root_path(self))
        print("self.root_path", self.root_path)
        # Bigquery instance
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.path = path 
        self.json_credential = json_credential

        # GitHubSource subclass
        GitHubSource.__init__(self, 
                        root_path=self.root_path,
                        github_repo_username=github_repo_username,
                        github_local_remote_name=github_local_remote_name,
                        github_commit_comment=github_commit_comment,
                        github_update_repo=github_update_repo,
                        github_repo_is_org=github_repo_is_org,
                        github_repo_branch=github_repo_branch
                    )
        self.github_local_remote_name = github_local_remote_name


        # Bigquery instance validation
        if self.path is not None:
            self.bigquery_client = BigquerySource(path=self.path).client
        elif self.json_credential:
            self.bigquery_client = BigquerySource(json_credential=self.json_credential).client
        

    # TODO: InfoSchema: query and return query result
    def info_schema(self):
        directory_name="parameters"
        query = """
            SELECT * 
            FROM `{project_id}.{dataset_id}`.INFORMATION_SCHEMA.PARAMETERS;
        """.format(project_id=self.project_id, dataset_id=self.dataset_id)
        query_job = self.bigquery_client.query(query)  # Make an API request.
        #results = query_job.result().to_dataframe()

        payload_list = list()
        for row in query_job:
            k = list(row.keys())
            v = list(row.values())
            payload_dict = dict()
            for i in range(len(k)):
                payload_dict[k[i]] = v[i]
            payload_list.append(payload_dict)
        
        InfoSchema.download_and_create_file(self,
                                ref_one_name='info-schema',
                                ref_two_name=directory_name,
                                directory_name=directory_name,
                                payload=payload_list,
                                fname=f"{self.project_id}_{self.dataset_id}_info-schema_parameters.json"
                                )
            
        if (self.github_update_repo):
            GitHubSource.git_push_folder(self)


if __name__ == "__main__":
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