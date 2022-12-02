import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from infra.spark_session import get_spark_session
from infra.jdbc import DataWarehouse, save_data

class PmExtract:
    FILE_DIR = '/final_data/pm/AIR_HOUR_2019.csv'
    @classmethod
    def extract_data(cls):
        pm = get_spark_session().read.csv(cls.FILE_DIR, encoding='UTF-8', header=True)
        pm8_df = pm.select((substring(col('data_dt'),1,8)).alias("DAY")
                    ,(substring(col("data_dt"),9,4)).alias("TIME")
                    ,col("loc_code").alias('LOC_CODE'),col("data_value").alias('PM8')) \
                .filter(col("item_code")==8)\
                .filter(col("data_state")==0) \
                .filter((substring(col('data_dt'),9,4)>='1000') & (substring(col("data_dt"),9,4)<='2200'))\
                .orderBy(['DAY','TIME','LOC_CODE'])
        # pm8_df.show()

        pm9_df = pm.select((substring(col('data_dt'),1,8)).alias("DAY")
                    ,(substring(col("data_dt"),9,4)).alias("TIME")
                    ,col("loc_code").alias('LOC_CODE'),col("data_value").alias('PM9')) \
                .filter(col("item_code")==9)\
                .filter(col("data_state")==0) \
                .filter((substring(col('data_dt'),9,4)>='1000') & (substring(col("data_dt"),9,4)<='2200')) \
                    .orderBy(['DAY','TIME','LOC_CODE'])
        # pm9_df.show()

        pm8_df.join(pm9_df,on=['DAY','TIME','LOC_CODE'],how='outer').show()



        # pm8_df = pm8_df.to_pandas_on_spark()
        # pm8_df = pm8_df.set_index(['DAY','TIME','pm8'])
        # pm8_df = pm8_df.stack()
        # pm8_df = pm8_df.unstack(3)
        # pm8_df = pm8_df.to_dataframe("미세먼지")
        # print(pm8_df)

        # pm9_df = pm9_df.to_pandas_on_spark()
        # pm9_df = pm9_df.set_index(['DAY','TIME','pm9'])
        # pm9_df = pm9_df.stack()
        # pm9_df = pm9_df.unstack(3)
        # pm9_df = pm9_df.to_dataframe("초미세먼지")
        # print(pm9_df)
