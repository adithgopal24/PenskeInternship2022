import os
import sys
import cli_args
import athena
import _pyspark as sparkhelp

import pyspark
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
    VectorIndexer,
)
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    DateType,
    DoubleType
)



def process_data(query_result_path, schema, partitions=None):
    ''' Read and transform data
    
        Write your implementation for project usecase
    '''
    df = spark.read.csv(
            (query_result_path),
            header=True,
            schema=schema,
            ignoreLeadingWhiteSpace=True,
            ignoreTrailingWhiteSpace=True
        )
    return df 

    
if __name__ == "__main__":
    # Spark context
    spark = SparkSession.builder.appName("PySparkApp").getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    
    # Parse CLI arguments
    args, _ = cli_args.parse_args()
    
    # Get dataset by running athena query
    if args.is_athena_dataset_definition:
        df = sparkhelp.get_athena_dataset(args.input_athena_path)
    else:
        query_execution_id = athena.get_athena_data(
            region_name=args.region_name,
            query_string=args.query_string,
            database=args.database,
            catalog=args.catalog,
            workgroup=args.workgroup)
        query_result_path, col_dtypes = athena.read_athena_data(query_execution_id, args.region_name)
        schema = sparkhelp.to_spark_schema(col_dtypes)
        df = process_data(query_result_path, schema)
        if args.partition_col:
            unique_model_keys = df.select(args.partition_col).distinct()
        
    # Save output
    sparkhelp.save_output(df, save_path=args.s3_output_path, partitions=args.partition_col)