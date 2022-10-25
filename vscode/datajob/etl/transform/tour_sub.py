from infra.spark_session import get_spark_session
from pyspark.sql.functions import col, regexp_replace, lit
from infra.jdbc import DataWarehouse, find_data

class TourSub:

    @classmethod
    def tour_sub(cls):
        df = find_data(DataWarehouse, 'T_TOURISM')
        df.show()
        tmp = df.select('T_IDX', 'T_NAME', 'ADDRESS', regexp_replace(col('STATION_NAME'), "역$", ''). alias('STATION_NAME')
                        , 'CATE', 'SUBCATE', 'LAT', 'LON')
        tmp.show()





# .select('LAT', 'LON', regexp_replace(col('STATION_NAME'), "(.*)(.{1})", 'r'). alias('STATION_NAME'))