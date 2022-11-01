import argparse
import csv
import os
import shutil
import sys
import time
import re
import datetime as dt
os.system('pip install s3fs')

import pandas as pd
import numpy as np
import json

# importing pq to convert to parquet
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
s3 = s3fs.S3FileSystem()


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
    '''Reads all input_data into and returns dict of dataframes dynamically from arg_parse data_dict flag'''
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

         
if __name__ == '__main__':
    args, _ = parse_args()
    data = read_input_files(args)
    dtypes = {"dist_num": "string"}
    
    join_key = ['dist_num', 'time_key']
    
    time_df = data['time']
    time_df.columns = time_df.columns.str.lower()
    time_df = time_df[['time_key', 'calendar_dt']]
    time_df = time_df.astype({"calendar_dt": "datetime64"})
    time_df.set_index("calendar_dt", inplace=True)
    
    # Rental Util
    ru = data['rental_util']
    ru.columns = ru.columns.str.lower()
    ru = ru.rename(columns={'rec_date': 'calendar_dt'})
    ru.set_index(['calendar_dt'], inplace=True)
    ru = ru.join(time_df)
    ru = ru.astype(dtypes)
    #ru.set_index(join_key, inplace=True)

    #
    staff = data['staff']
    staff = staff.rename(columns={"sched_date_key": "time_key"})
    staff = staff.astype(dtypes)
    #staff.set_index(join_key, inplace=True)

    fd = data['facility_detail']
    fd = fd.astype(dtypes)
    
    
    targets = data['target']
    targets = targets.rename(columns={'rsc_date': 'calendar_dt'})
    targets = targets.astype({"calendar_dt": "datetime64"})
    targets = targets.merge(time_df, on='calendar_dt')
    targets = targets.astype(dtypes)
    #targets = targets.set_index(join_key)

    # Join all inputs
    total_df = ru.merge(staff, on=join_key)
    total_df = total_df.astype(dtypes)
    total_df = total_df.merge(fd, on='dist_num')
    total_df = total_df.merge(targets, on=join_key)
    total_df.to_csv(args.output_path)
