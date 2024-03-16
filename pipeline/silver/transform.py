from pyspark.sql import SparkSession, functions as F
from pyspark.sql import DataFrame, types as T
    
def prep_abmatchdframe(sps: SparkSession, df_ab: DataFrame) -> DataFrame:
    
    df_abstr = df_ab.select(
        'UPRN',
        F.substring_index('POSTCODE_LOCATOR', ' ', -1).alias('OUTCODE'),
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
