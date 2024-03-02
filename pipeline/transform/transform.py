from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame, types as T

def read_csv(sps: SparkSession, csvpath: str) -> DataFrame:
    
    df_csv = sps.read.csv(csvpath, header=True, inferSchema=True)
    
    return df_csv

def read_parquet(sps: SparkSession, parquetpath: str) -> DataFrame:
    
    df_parquet = sps.read.parquet(parquetpath)
    
    return df_parquet

def write__repart_parquet(sps: SparkSession, df_parquet: DataFrame, numparts: int, parquetpath: str):
    
    df_parquet.repartition(numparts).write.mode('overwrite').parquet(parquetpath)

def write_parquet(sps: SparkSession, df_parquet: DataFrame, parquetpath: str):

    df_parquet.write.mode('overwrite').parquet(parquetpath)
    
def prep_abmatchdframe(sps: SparkSession, df_ab: DataFrame) -> DataFrame:
    
    df_abstr = df_ab.select(
        'UPRN',
        F.substring_index('POSTCODE_LOCATOR', ' ', 1).alias('OUTCODE'),
        'LOCALITY',
        F.concat_ws(' ',
                    F.coalesce('RM_ORGANISATION_NAME','LA_ORGANISATION'),
                    'DEPARTMENT_NAME',
                    'SUB_BUILDING_NAME',
                    'BUILDING_NAME',
                    'BUILDING_NUMBER',
                    F.coalesce('STREET_DESCRIPTION','THOROUGHFARE'),
                    'LOCALITY',
                    F.coalesce('TOWN_NAME','POST_TOWN'),
                    'ADMINISTRATIVE_AREA',
                    'POSTCODE_LOCATOR'
                    ).alias('ADDRESS')
        )
    
    return df_abstr
