import os
from mdutils.mdutils import MdUtils
from beepbeep_txflowutils.utils.utils import Utilities


MD_DIR = os.path.join(Utilities()._root_path(), 'markdown')
MD_DOCS_DIR = os.path.join(MD_DIR, "docs")
os.makedirs(MD_DOCS_DIR, exist_ok=True)
FILENAME_PATH = os.path.join(MD_DOCS_DIR, "functions-transformation-template.md")
mdFile = MdUtils(file_name=FILENAME_PATH, title='Data Transformation Functions')
mdFile.create_md_file()

#  ********************* FUNTION TRANSFORMATION CONTENT ***************************
# Data Transformation Functions BLOCK
dtf_h1 = mdFile.new_header(level=1, title=f'Data Transformation Functions')
dtf_txt =  mdFile.write("These functions form the core of the framework and aim to automate and simplify common data transformation patterns.\n")
Data_Transformation_Functions = f'{dtf_h1}{dtf_txt}'


# De-Duplication BLOCK
de_duplication_h2 = mdFile.new_header(level=2, title='De-Duplication')
de_duplication_txt = mdFile.write("Robust de-duplication is typically executed by leveraging a row hash function, as unique identifiers are not enforced in BigQuery.\n")

de_duplication_call_ref = mdFile.new_header(level=3, title="`txflow.transform.remove_duplicates`")
de_duplication_call_ref_list_of_strings = ["function_group", "function_name", "function_output", "description"]
for x in range(1):
    de_duplication_call_ref_list_of_strings.extend(["`transform`", "`remove_duplicates`", "e.g. VIEW", "Creates a view with duplicates removed.  Typically follows addition of a rowhash based on a specified subset of columns (`txflow.prepare.add_rowhash`)"])
de_duplication_call_table = mdFile.new_table(columns=4, rows=2, text=de_duplication_call_ref_list_of_strings, text_align='left')

de_duplication_call_syntax_h4 = mdFile.new_header(level=4, title="Call Syntax")
de_duplication_call_syntax_script = mdFile.write("``` sql \n\
CALL txflow.transform.remove_duplicates( \n\
     'source_ref__STRING', \n\
     'destination_ref__STRING', \n\
     'partition_by_columns__ARRAY<STRING>', \n\
     'order_by_columns_with_optional_sort__ARRAY<STRING>' \n\
) \n\
```")

de_duplication_args_h4 = mdFile.new_header(level=4, title="Arguments")
de_duplication_args_h4_list_of_strings = ["argument", "datatype", "description"]
de_duplication_list_content = [
            ["`source_ref`", "`STRING`", "Input table or view reference"],
            ["`destination_ref`", "`STRING`", "New view reference"],
            ["`partition_by`", "`ARRAY <STRING>`", "Partition by column(s)"],
            ["`order_by_columns_with_optional_sort`", "`ARRAY <STRING>`", "Sort column(s) with optional ASC/DESC (default ASC if omitted)"]
        ]
# len(de_duplication_list_content) return 4
for x in range(len(de_duplication_list_content)): # range(4) => 0, 1, 2, 3
    de_duplication_args_h4_list_of_strings.extend(de_duplication_list_content[x])
de_duplication_args_h4_table = mdFile.new_table(columns=3, rows=5, text=de_duplication_args_h4_list_of_strings, text_align='left')

de_duplication_example_h4 = mdFile.new_header(level=4, title="Example")
de_duplication_example_script = mdFile.write("``` sql \n\
CALL txflow.function_group.function_name( \n\
     'project.dataset.input_table', \n\
     'project.dataset.output_table', \n\
     ['rowhash'], \n\
     ['timestamp DESC', 'additional_sort_column ASC'] \n\
) \n\
```")

De_Duplication = f'{de_duplication_h2}{de_duplication_txt}{de_duplication_call_ref}\
    {de_duplication_call_table}{de_duplication_call_syntax_h4}{de_duplication_call_syntax_script}\
    {de_duplication_args_h4}{de_duplication_args_h4_table}\
    {de_duplication_example_h4}{de_duplication_example_script}\
    '.format(
    {de_duplication_h2},
    {de_duplication_txt},
    {de_duplication_call_ref},
    {de_duplication_call_table},
    {de_duplication_call_syntax_h4},
    {de_duplication_call_syntax_script},
    {de_duplication_args_h4},
    {de_duplication_args_h4_table},
    {de_duplication_example_h4},
    {de_duplication_example_script}
)


# JSON Extraction
json_extraction = mdFile.new_header(level=2, title="JSON Extraction")
json_extraction_txt_1 = mdFile.write(f'BigQuery can extract data from JSON stored in STRING fields and restructure it into scalar or array columns using \
    {mdFile.new_inline_link(link="https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions", text="JSON functions in Standard SQL.")}\n\n')
json_extraction_txt_2 = mdFile.write(f'However, extraction of the data requires manual inspection of the exact structure and manually entering the exact path for each element, in addition to understanding whether it is a scalar (single) value or array of elements.\n\n')
json_extraction_txt_3 = mdFile.write(f'These utilities automate the inspection and enable users to automatically and systematically extract JSON into native BigQuery data structures .')

json_extraction_call_ref = mdFile.new_header(level=3, title="`txflow.transform.decode_json`")
json_extraction_call_ref_list_of_strings = ["function_group", "function_name", "function_output", "description"]
for x in range(1):
    json_extraction_call_ref_list_of_strings.extend(["`transform`", "`decode_json`", "VIEW", "Decodes the top level of a JSON object, automatically detecting the structure and applying the correct JSON extraction function to each element."])
json_extraction_call_table = mdFile.new_table(columns=4, rows=2, text=json_extraction_call_ref_list_of_strings, text_align='left')
json_extraction_call_txt_note = mdFile.write(f'\nNote that the schema used to generate the decoder SQL is detected from a single JSON object.')

json_extraction_call_syntax_h4 = mdFile.new_header(level=4, title="Call Syntax")
json_extraction_call_syntax_script = mdFile.write("``` sql \n\
CALL txflow.transform.decode_json( \n\
     'source_ref__STRING', \n\
     'destination_ref__STRING', \n\
     'json_column__STRING', \n\
     'order_by__STRING' \n\
) \n\
```")

json_extraction_args_h4 = mdFile.new_header(level=4, title="Arguments")
json_extraction_args_h4_list_of_strings = ["argument", "datatype", "description"]
json_extraction_list_content = [
            ["`source_ref`", "`STRING`", "Source table or view reference"],
            ["`destination_ref`", "`STRING`", "Reference for new output view"],
            ["`json_column`", "`STRING`", "Column containing JSON string"],
            ["`order_by`", "`STRING`", "'Order by' column used to determine which object is used for schema detection"]
        ]
# len(json_extraction_list_content) return 4
for x in range(len(json_extraction_list_content)): # range(4) => 0, 1, 2, 3
    json_extraction_args_h4_list_of_strings.extend(json_extraction_list_content[x])
json_extraction_args_h4_table = mdFile.new_table(columns=3, rows=5, text=json_extraction_args_h4_list_of_strings, text_align='left')


JSON_Extraction = f'{json_extraction}{json_extraction_txt_1}{json_extraction_txt_2}{json_extraction_txt_3}{json_extraction_call_ref}\
    {json_extraction_call_table}{json_extraction_call_txt_note}{json_extraction_call_syntax_h4}{json_extraction_call_syntax_script}\
    {json_extraction_args_h4}{json_extraction_args_h4_table}\
    '.format(
    {json_extraction},
    {json_extraction_txt_1},
    {json_extraction_txt_2},
    {json_extraction_txt_3},
    {json_extraction_call_ref},
    {json_extraction_call_table},
    {json_extraction_call_txt_note},
    {json_extraction_call_syntax_h4},
    {json_extraction_call_syntax_script},
    {json_extraction_args_h4},
    {json_extraction_args_h4_table}
)


# Array Manipulation
array_manipulation = mdFile.new_header(level=2, title="Array Manipulation")
array_manipulation_txt_1 = mdFile.write(f'The syntax for working with arrays in BigQuery can be difficult to master (see  \
    {mdFile.new_inline_link(link="https://cloud.google.com/bigquery/docs/reference/standard-sql/array_functions", text="Array functions in Standard SQL.")} \
        , despite the comprehensive documentation and examples (e.g. \
            {mdFile.new_inline_link(link="https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions", text="Working with arrays in Standard SQL.")}\n\n')
array_manipulation_txt_2 = mdFile.write(f'These functions automate SQL generation for typical array manipulation patterns to simplify and accelerate the transformation development process.\n\n')


f'{array_manipulation}{array_manipulation_txt_1}{array_manipulation_txt_2}'

array_manipulation_call_ref = mdFile.new_header(level=3, title="`txflow.transform.unnest_array`")
array_manipulation_call_ref_list_of_strings = ["function_group", "function_name", "function_output", "description"]
for x in range(1):
    array_manipulation_call_ref_list_of_strings.extend(["`transform`", "`unnest_array`", "VIEW", "Unnests a single array field and flattens the output into a standard view, with optional exclusions of columns which should not be repeated in the output view."])
array_manipulation_call_table = mdFile.new_table(columns=4, rows=2, text=array_manipulation_call_ref_list_of_strings, text_align='left')

array_manipulation_call_syntax_h4 = mdFile.new_header(level=4, title="Call Syntax")
array_manipulation_call_syntax_script = mdFile.write("``` sql \n\
CALL txflow.transform.unnest_array( \n\
     'source_ref__STRING', \n\
     'destination_ref__STRING', \n\
     'unnest_array_column__STRING', \n\
     ['exclude_columns__ARRAY_STRING'] \n\
) \n\
```")

array_manipulation_args_h4 = mdFile.new_header(level=4, title="Arguments")
array_manipulation_args_h4_list_of_strings = ["argument", "datatype", "description"]
array_manipulation_list_content = [
            ["`source_ref`", "`STRING`", "Source table or view"],
            ["`destination_ref`", "`STRING`", "Reference of new output view"],
            ["`unnest_array_column`", "`STRING`", "Column containing array to be unnested"],
            ["`exclude_columns`", "`ARRAY <STRING>`", "Columns to exclude from output table. To include all columns you must set this as an empty array ('[]')"]
        ]
# len(array_manipulation_list_content) return 4
for x in range(len(array_manipulation_list_content)): # range(4) => 0, 1, 2, 3
    array_manipulation_args_h4_list_of_strings.extend(array_manipulation_list_content[x])
array_manipulation_args_h4_table = mdFile.new_table(columns=3, rows=5, text=array_manipulation_args_h4_list_of_strings, text_align='left')


Array_Manipulation = f'{array_manipulation}{array_manipulation_txt_1}{array_manipulation_txt_2}{array_manipulation_call_ref}\
    {array_manipulation_call_table}{array_manipulation_call_syntax_h4}{array_manipulation_call_syntax_script}\
    {array_manipulation_args_h4}{array_manipulation_args_h4_table}\
    '.format(
    {array_manipulation},
    {array_manipulation_txt_1},
    {array_manipulation_txt_2},
    {array_manipulation_call_ref},
    {array_manipulation_call_table},
    {array_manipulation_call_syntax_h4},
    {array_manipulation_call_syntax_script},
    {array_manipulation_args_h4},
    {array_manipulation_args_h4_table}
)

function_transformation_content = f'{Data_Transformation_Functions}{De_Duplication}{JSON_Extraction}{Array_Manipulation}'\
    .format(
        {Data_Transformation_Functions},{De_Duplication},{JSON_Extraction},{Array_Manipulation}
    )


if __name__ == "__main__":
    with open(FILENAME_PATH, "w") as f:
        f.write(function_transformation_content)
