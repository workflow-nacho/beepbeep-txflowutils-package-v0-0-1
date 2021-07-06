import os
import json

from numpy import divide
from beepbeep_txflowutils.secrets.config_setting import config_secret
from mdutils.mdutils import MdUtils
from beepbeep_txflowutils.utils.utils import Utilities


class MarkdownRender(Utilities):
    mdFile = None
    root_path = Utilities()._root_path()
    FILENAME_PATH = None
    
    # Methods
    def md_bucket_dir(self,
            folder_render_name: str = None,
            file_render_name: str = None
        ):
        """
        Function to create Markdown folder and Markdown format file.
        - parameters:
            - folder_render_name (required): folder name to save the markdown file rendered  
            - file_render_name (required): file name to write the markdown content.
        """
        self.folder_render_name=folder_render_name
        self.file_render_name=file_render_name

        if (self.folder_render_name or self.file_render_name) is None:
            print("\nFolder and File name is required!\n")
        else:
            MD_DIR = os.path.join(self.root_path, 'markdown')
            MD_DOCS_DIR = os.path.join(MD_DIR, f'{self.folder_render_name}')
            os.makedirs(MD_DOCS_DIR, exist_ok=True)
            self.FILENAME_PATH = os.path.join(MD_DOCS_DIR, f"{self.file_render_name}.md")

            if (isinstance(self.mdFile, MdUtils) is not None):
                self.mdFile = MdUtils(file_name=self.FILENAME_PATH, title=f'Markdown file: {self.file_render_name} with not content')
                self.mdFile.create_md_file()
        print(f"Directory named '{self.folder_render_name}' and file named '{self.file_render_name}.md' have been created succesfully!")

    # Built Markdown template
    def data_transformation_function_template(self,
            dtf_main_h1: str = None,
            dtf_main_description: str = None,
            dtf_subheader_h2: str = None,
            dtf_subheader_h2_description: str = None
        ):
        self.dtf_main_h1=dtf_main_h1
        self.dtf_main_description=dtf_main_description
        self.dtf_subheader_h2=dtf_subheader_h2
        self.dtf_subheader_h2_description=dtf_subheader_h2_description

        self.markdown_content = None
        self.markdown_content_concat_str = None
        self.markdown_content_concat_list = []
        """
        Funtion to create the markdown template of the data transformation function.
        - parameters:
            - dtf_main_h1 (require a str | default None): main title of the template
            - dtf_main_description (require a str | default None): main description of the template
            - dtf_subheader_h2 (require a str | default None): the subheader title of the template
            - dtf_subheader_h2_description (require a str | default None): the subheader description of the template
        """

        # TODO: Where will we get this?
        function_output = "e.g. VIEW"
 
        # CALL SYNTAX and ARGUMENTS PARAMETERS: 
        # TODO: This list contain description of the argument, datatype and function.
        call_syntax_args_list = [
            "TODO: Here the relevant description pending to add"
            #"Input table or view reference",
            #"New view reference",
            #"Partition by column(s)"
        ]

        def dynamic_md_template(self):
            #  ********************* FUNTION TRANSFORMATION CONTENT TEMPLATE ***************************
            # Data Transformation Functions MAIN BLOCK: Main header BLOCK
            dtf_main_h1 = self.mdFile.new_header(level=1, title=f'{self.dtf_main_h1}')
            dtf_main_description =  self.mdFile.write(f"{self.dtf_main_description}\n")
            dtf_main_block = f'{dtf_main_h1}{dtf_main_description}'

            # SUBHEADER BLOCK
            #dtf_subheader_h2 = self.mdFile.new_header(level=2, title=f'{self.dtf_subheader_h2}')
            #dtf_subheader_h2_description = self.mdFile.write(f"{self.dtf_subheader_h2_description}\n")
            # SUBHEADER BLOCK
            dtf_subheader_h2 = self.mdFile.new_header(level=2, title=f'Title of the F. Group: {function_group} and F. Name: {function_name}')
            dtf_subheader_h2_description = self.mdFile.write(f"Description of the F. Group: {function_group} and F. Name: {function_name}\n")

            dtf_call_ref = self.mdFile.new_header(level=3, title=f"`{function_project_ref}.{function_group}.{function_name}`")
            dtf_call_ref_list_of_strings = ["function_group", "function_name", "function_output", "description"]
            
            dtf_call_ref_content = [
                        [f"`{function_group}`", f"`{function_name}`", f"{function_output}", f"{function_description}"]
                    ]
            # column_num: Number of columns of the table
            dtf_call_ref_columns_num = len(dtf_call_ref_list_of_strings)
            # rows_num: Number of rows of the table. Plus 1 for the head column.
            dtf_call_ref_rows_num = len(dtf_call_ref_content) + 1
            for x in range(len(dtf_call_ref_content)):
                dtf_call_ref_list_of_strings.extend(
                    dtf_call_ref_content[x]
                )
            dtf_call_table = self.mdFile.new_table(columns=dtf_call_ref_columns_num, rows=dtf_call_ref_rows_num, text=dtf_call_ref_list_of_strings, text_align='left')
            dtf_call_syntax_h4 = self.mdFile.new_header(level=4, title="Call Syntax")
            dtf_call_syntax_script = self.mdFile.write(f"{dtf_call_syntax_script_format}")


            dtf_args_h4 = self.mdFile.new_header(level=4, title="Arguments")
            dtf_args_h4_list_of_strings = ["argument", "datatype", "description"]
            
            # column_num: Number of columns of the table
            columns_num = len(dtf_args_h4_list_of_strings)
            # rows_num: Number of rows of the table. Plus 1 for the head column.
            rows_num = len(dtf_args_list_content) + 1
            # len(dtf_args_list_content) return 4
            for x in range(len(dtf_args_list_content)): # range(4) => 0, 1, 2, 3
                dtf_args_h4_list_of_strings.extend(dtf_args_list_content[x])
            dtf_args_h4_table = self.mdFile.new_table(columns=columns_num, rows=rows_num, text=dtf_args_h4_list_of_strings, text_align='left')

            dtf_example_h4 = self.mdFile.new_header(level=4, title="Example")
            dtf_example_script = self.mdFile.write("```sql \n\
            CALL txflow.function_group.function_name( \n\
                'project.dataset.input_table', \n\
                'project.dataset.output_table', \n\
                ['rowhash'], \n\
                ['timestamp DESC', 'additional_sort_column ASC'] \n\
            ) \n```")

            dtf = f'{dtf_subheader_h2}{dtf_subheader_h2_description}{dtf_call_ref}\
                {dtf_call_table}{dtf_call_syntax_h4}{dtf_call_syntax_script}\
                {dtf_args_h4}{dtf_args_h4_table}\
                {dtf_example_h4}{dtf_example_script}\
                '.format(
                {dtf_subheader_h2},
                {dtf_subheader_h2_description},
                {dtf_call_ref},
                {dtf_call_table},
                {dtf_call_syntax_h4},
                {dtf_call_syntax_script},
                {dtf_args_h4},
                {dtf_args_h4_table},
                {dtf_example_h4},
                {dtf_example_script}
            )
            divider = self.mdFile.write("\n--------------------------------\n\n")

            self.markdown_content = f'{dtf}{divider}'\
                .format(
                    {dtf},
                    {divider}
                )
            self.markdown_content_concat_str = self.markdown_content
            self.markdown_content_concat_list.append(self.markdown_content)
            self.write_md_rendered(self.FILENAME_PATH,  self.markdown_content_concat_list)
            

        # Read files from parameters directory
        param_dir_path = os.path.join(self.root_path, "parameters")
        dtf_main_h1 = self.mdFile.new_header(level=1, title=f'{self.dtf_main_h1}')
        dtf_main_description =  self.mdFile.write(f"{self.dtf_main_description}\n")
        dtf_main_block = f'{dtf_main_h1}{dtf_main_description}'
        self.markdown_content = f'{dtf_main_block}'\
                .format(
                    {dtf_main_block}
                )
        self.markdown_content_concat_str = self.markdown_content
        self.markdown_content_concat_list.append(self.markdown_content)
        for dir_name in os.listdir(param_dir_path):
            if dir_name.endswith('.json'):
                param_file_path = os.path.join(param_dir_path, dir_name) # get absolute file name. e.g: c:/parameters/filename.json
                with open(param_file_path, 'r') as read_f:
                    f_content_items = json.loads(read_f.read()) # get the list of dicts
                    temp_list = list()

                    # Loop around .json files
                    for f_content_item in f_content_items:
                        # To loop only into the current project name
                        #if f_content_item['specific_catalog'] == config_secret['PROJECT_ID']:
                        temp_list.append(f_content_item['specific_name'])
                        temp_list = list(dict.fromkeys(temp_list)) # Remove duplicated

                    payload_list = list() # Out of the loop in order to create only one list with all dicts. Otherwise, it creates separate list per each f_content_item dict
                    for i in range(len(temp_list)):
                        payload_dict = dict() 
                        parameter_name_list = list() # To append the value of the parameter_name attr
                        data_type_list = list() # To append the value of the data_type attr
                        for f_content_item in f_content_items:
                            #for k, v in f_content_item.items(): # Not neccesary
                            if f_content_item["specific_name"] == temp_list[i]:
                                payload_dict['specific_catalog'] = f_content_item['specific_catalog']
                                payload_dict['specific_schema'] = f_content_item['specific_schema']
                                payload_dict['specific_name'] = f_content_item['specific_name']
                                parameter_name_list.append(f_content_item['parameter_name'])
                                payload_dict['parameter_name'] = parameter_name_list
                                data_type_list.append(f_content_item['data_type'])
                                payload_dict['data_type'] = data_type_list
                        payload_list.append(payload_dict)
                    if len(payload_list) > 0:
                        for i in range(len(payload_list)):
                            function_project_ref = payload_list[i]['specific_catalog']
                            function_group = payload_list[i]['specific_schema']
                            function_name = payload_list[i]['specific_name']
                            function_description = f"Function name: `{payload_list[i]['specific_name']}`"
                            param_name_op = payload_list[i]['parameter_name']
                            data_type_op = payload_list[i]['data_type']
                            dtf_call_syntax_script_blk_2=[]
                            
                            dtf_args_list_content= []
                            for i in range(len(param_name_op)):
                                # create dynamic variables name
                                if param_name_op[i] is not None:
                                    #globals()[f'param_name_op_{i+1}'] = param_name_op[i]
                                    #globals()[f'call_syntax_args_op_{i+1}'] = call_syntax_args_list[i]
                                    dtf_call_syntax_script_blk_2.append(f"{param_name_op[i]}__{data_type_op[i]}")
                                    dtf_args_list_content.append([f"`{param_name_op[i]}`", f"`{data_type_op[i]}`", f"`{call_syntax_args_list[0]}`"])
                            # dtf_call_syntax_script_format: Separate in three blocks
                            dtf_call_syntax_script_blk_1=f"```sql \n CALL {function_project_ref}.{function_group}.{function_name}( \n "
                            dtf_call_syntax_script_blk_3=f"\n)\n```"
                            dtf_call_syntax_script_format = f"{dtf_call_syntax_script_blk_1}" + '\n'.join([f"\t'{str(lst)}'" for lst in dtf_call_syntax_script_blk_2]) + f"{dtf_call_syntax_script_blk_3}"

                            #for i in range(len(data_type_op)):
                            #    # create dynamic variables name
                            #    globals()[f'data_type_op_{i+1}'] = data_type_op[i]
                            dynamic_md_template(self)

    # Write into Markdown file the markdown content rendered
    def write_md_rendered(self, filename_path: str, markdown_content: str):
        if os.path.exists(filename_path):
            with open(filename_path, "w") as f:
                for md_l_content in markdown_content:
                    f.write(md_l_content)
                print(f"Markdown content have been written succesfully!")
            return True
        else:
            print(f"Filename: {filename_path} doesn't exist")
            return False
            

if __name__ == "__main__":
    md_render_obj = MarkdownRender()
    md_render_obj.md_bucket_dir(
        folder_render_name=config_secret['folder_render_name'], 
        file_render_name=config_secret['file_render_name']
    )
    md_render_obj.data_transformation_function_template(
        dtf_main_h1=config_secret['dtf_main_h1'],
        dtf_main_description=config_secret['dtf_main_description'],
        dtf_subheader_h2=config_secret['dtf_subheader_h2'],
        dtf_subheader_h2_description=config_secret['dtf_subheader_h2_description']
    )
    pass

