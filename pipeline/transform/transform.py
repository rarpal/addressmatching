from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame, types as T

def read_addressbase(sps: SparkSession, abpath: str) -> DataFrame:
    #df_ab = sps.read.csv('../../../data/ab-plus_SX9090_withhead.csv', header=True, inferSchema=True)
    
    df_ab = sps.read.csv(abpath, header=True, inferSchema=True)
    
    return df_ab

def read_buildingaddress(sps: SparkSession, bldadrpath: str) -> DataFrame:
    
    df_bldadr = sps.read.csv(bldadrpath, header=True, inferSchema=True)
    
    return df_bldadr

def prep_abmatchdframe(sps: SparkSession, df_ab: DataFrame) -> DataFrame:
    
    df_abstr = df_ab.select(
        'UPRN',
        F.substring_index('POSTCODE_LOCATOR', ' ', 1).alias('OUTCODE'),
        'LOCALITY',
        F.concat_ws('|',
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

def write_abmatchdframe(sps: SparkSession, df_ab: DataFrame, numparts: int, abinpath: str):
    
    df_ab.repartition(numparts).write.mode('overwrite').parquet(abinpath)
    
def write_bldadrmatchdframe(sps: SparkSession, df_bldadr: DataFrame, bldadrinpath: str):

    df_bldadr.write.mode('overwrite').parquet(bldadrinpath)
    