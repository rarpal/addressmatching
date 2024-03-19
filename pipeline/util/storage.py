from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame, types as T

def read_csv(sps: SparkSession, csvpath: str) -> DataFrame:
    
    df_csv = sps.read.csv(csvpath, header=True, inferSchema=True)
    
    return df_csv

def read_parquet(sps: SparkSession, parquetpath: str) -> DataFrame:
    
    df_parquet = sps.read.parquet(parquetpath)
    
    return df_parquet

def write_repart_parquet(sps: SparkSession, df_parquet: DataFrame, numparts: int, parquetpath: str):
    
    df_parquet.repartition(numparts).write.mode('overwrite').parquet(parquetpath)

def write_parquet(sps: SparkSession, df_parquet: DataFrame, parquetpath: str):

    df_parquet.write.mode('overwrite').parquet(parquetpath)

def write_delta(sps: SparkSession, df_delta: DataFrame, deltapath: str):

    df_delta.write.format('delta').mode('overwrite').save(deltapath)

def write_deltatable(sps: SparkSession, df_delta: DataFrame, deltatable: str):

    df_delta.write.format('delta').mode('overwrite').saveAsTable(deltatable)
