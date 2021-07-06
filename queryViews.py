import os
from datetime import datetime
from beepbeep_txflowutils.secrets.config_setting import config_secret
from beepbeep_txflowutils.auth.service_account import BigquerySource
from beepbeep_txflowutils.auth.info_schema import InfoSchema
from beepbeep_txflowutils.github_source.github_source import GitHubSource
from beepbeep_txflowutils.utils.utils import Utilities


class BigqueryViews(BigquerySource, InfoSchema, GitHubSource, Utilities):
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
        self.root_path = os.path.join(Utilities._root_path(self))
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
        directory_name = "query_views"

        dataset_list_table = self.bigquery_client.get_dataset(self.dataset_id)  # Make an API request.
        full_dataset_id = "{}.{}".format(dataset_list_table.project, dataset_list_table.dataset_id)
        directory_name = "query_views"

        print("Got dataset '{}'.".format(full_dataset_id))
        print("DATASET MODIFIED AT: ", dataset_list_table.modified)

        # View tables id and tables type in dataset.
        tables = list(self.bigquery_client.list_tables(dataset_list_table))  # Make an API request(s).

        if tables:
            for table in tables:
                if table.table_type == "VIEW":
                    view = self.bigquery_client.get_table(table.reference)
                    date_format = datetime.strftime(view.modified, '%Y-%m-%d-%H-%M-%S')

                    InfoSchema.download_and_create_file(self,
                            ref_one_name=view.modified,
                            ref_two_name=view.table_id,
                            directory_name=directory_name,
                            payload=view.view_query,
                            fname=f"{self.project_id}_{self.dataset_id}_{view.table_id}_{date_format}.txt"
                            )
                else:
                    print("NOT VIEW TABLE TYPE BUT \t{}".format(table.table_type))
        else:
            print("\tThis dataset does not contain any tables.")

            
        if (self.github_update_repo):
            GitHubSource.git_push_folder(self)


if __name__ == "__main__":
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