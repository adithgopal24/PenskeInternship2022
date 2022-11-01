import pandas as pd
import numpy as np
import argparse
import sys
import os
#os.system('pip install fsspec')
sys.path.extend(['/opt/ml/processing/input/instance', '/opt/ml/processing/input/utils', '/opt/ml/processing/input/s3', '/opt/ml/processing/input/athena'])
import s3 as s3_helper
import athena as at
import utils as ut
os.system('pip install s3fs')


# READ YAML FILES AND STORE RELEVANT PATHS in dictionaries
#dataset_cfg = ins.read_config('../config/datasets.yaml') 
#config_cfg = ins.read_config('../config/config.yaml')
#sql_cfg = ins.read_config('../config/sql.yaml')

def read_args() :
    '''
        Reads parsed arguments 
        Returns:
            args (class object):
    '''
    parser = argparse.ArgumentParser(description="app inputs and outputs")
    parser.add_argument('--s3_input_path', type=str, help='s3 input path')
    parser.add_argument('--s3_input_path1', type=str, help='s3 target output path')
    args = parser.parse_args()
    return args

def get_query_strings(args):
    '''
        Preprocess unit data
        Arguments:
            args (class object):
        Returns:
            units (pd.DataFrame):
        Notes:
            Returning columns: [unit_num, vin, unit_key, unit_category, unit_type,
                                unit_size, model_year, make_code, account_code_group1]
    '''
    input_file = [args.s3_input_path]
    querystrings = ut.read_query_file_s3(input_files)
    units = s3_helper.read_parquet_from_path(path=args.s3_input_path) 
    return querystrings

if __name__ == '__main__':
    args = read_args()
    #reading files manually
    #input_files = [dataset_cfg['raw']['base_query']['input'], dataset_cfg['raw']['engine_hours']['input'], 
                   #dataset_cfg['raw']['postsaleissue']['input'],
                   #dataset_cfg['raw']['repair_base']['input']]
    input_files = ['s3://pske-stg-advanalytics/Projects/Unit_Sale_Risk_Interns/Data/Athena_query/base_query.txt']
    output_paths = ['s3://pske-stg-advanalytics/Projects/Unit_Sale_Risk_Interns/Data/Interim/base_query/']
    #output_paths = [dataset_cfg['raw']['base_query']['output_paths'], dataset_cfg['raw']['engine_hours']['output_paths'], 
                   #dataset_cfg['raw']['postsaleissue']['output_paths'],
                   #dataset_cfg['raw']['repair_base']['output_paths']]

    # passing Athena variables
    database=['ptledw_playarea', 'ptl_maintenance', 'ptl_connfleet', 'proactive_maint_db', 'ptl_adhoc']
    workgroup='WG-Advanalytics'
    tablename=['d_location_master','constellation_rules','ufch_depup', 'archive_pd_inference_scenario_output', 'corp_foilanl'] 
    query_strings = ['SELECT Det.PARTITION_KEY, ltrim(rtrim(unt.UNIT_NUM)) AS UNIT_NUM, Det.UDI_ACTUAL_VALUE, ident.UDI_CODE, ident.UDI_DESCRIPTION FROM ptledw_playarea.F_UNIT_UDI_DETAILS Det LEFT JOIN ptledw_playarea.D_UNIT unt ON det.UNIT_KEY = unt.UNIT_KEYJOIN ptledw_playarea.D_UNIQDEV_IDENT ident ON Det.UDI_KEY = ident.UDI_KEY WHERE ident.UDI_CODE = 10 AND ltrim(rtrim(unt.UNIT_NUM)) <> \'UNK\' AND unt.UNIT_SOLD_DATE BETWEEN DATE \'2019-01-01\' AND current_date']
   #query_strings = ut.read_query_file(input_files)
    df = at.get_datasets_from_athena(region = 'us-east-1', query_strings = query_strings, 
                                 database = database[0], catalog = 'AwsDataCatalog', 
                                 workgroup = workgroup)
    print(df[0].shape)
    s3_helper.persist_files_to_path(dfs=dfs,
                       paths = output_paths,
                       filetype='parquet')
    #s3_helper.read_multiple_parquet_files(output_paths)
    #query_strings = ut.read_query_file(input_file)
'''
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=[], type=str, help="s3 bucket")
    parser.add_argument("--prefix", default=[], type=str, help="s3 key prefix")
    parser.add_argument("--model_name_prefix", default=[], type=str, help="model name")
    parser.add_argument("--model_data_root", type=str, help="s3 model data for each segmentation")
    parser.add_argument('--filenames', nargs='+', default=[], help="filename[s] for input data")
    parser.add_argument('--data_dict', default=[], type=json.loads, help="key-value pair for input name and filepath on local instance")
    # Local config 
    parser.add_argument('--input_path', type=str, default='/opt/ml/processing/input')
    parser.add_argument('--output_path', type=str, default='/opt/ml/processing/output')
    return parser.parse_known_args()

def read_input_files(args):
    #Reads all input_data into and returns dict of dataframes dynamically from arg_parse data_dict flag
    data_dict = args.data_dict
    # Create local file path for 
    data_dict = {key: os.path.join(args.input_path, filename) for key, filename in data_dict.items()}
    dfs = {key: read_parquet_dir(filename) for key, filename in data_dict.items()}
    return dfs

def read_s3_input_files(args):
    data_dict = args.data_dict
    dfs = {key: pd.read_parquet(filename) for key, filename in data_dict.items()}
    return dfs

def read_parquet_dir(path):
    df = pq.ParquetDataset(path_or_paths=path,
                           filesystem=s3
                          )
    df = df.read_pandas().to_pandas()
    return df
'''