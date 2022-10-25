from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from pyspark.sql.functions import *

class TourismRestTransformt:

    @classmethod
    def transform(cls):
        sub = find_data(DataWarehouse,'SUBWAY')
        rest = get_spark_session().read.csv('/final_data/subway/서울시_맛집.csv', encoding='CP949', header=True)
        tourism = rest.join(sub, on = 'STATION_NAME').select('NAME',col('ADDRESS_1').alias('ADDRESS'),'CATE','SUBCATE'
                                                                ,col('LAT_1').cast('float').alias('LAT'),col('LON_1').cast('float').alias('LON'),'S_IDX')
        # tourism.show()

        save_data(DataWarehouse, tourism, 'TOURISM')