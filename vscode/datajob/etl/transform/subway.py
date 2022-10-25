from infra.spark_session import get_spark_session
from infra.jdbc import DataWarehouse, find_data, save_data
from pyspark.sql.functions import col, regexp_replace

class SubwayAdd:

    path_add = '/final_data/subway/subway_add.csv'
    path_coord = '/final_data/subway/subway_coordinate.csv'

    @classmethod
    def sub_add(cls):
        sub_add_df = get_spark_session().read.csv(cls.path_add, encoding='UTF-8', header=True)
        sub_add_df.show()

        sub_add_df = sub_add_df.select(regexp_replace(col('역명'), r'\([^)]*\)', '').alias('역명')
                                        , regexp_replace(col('도로명주소'), r'\([^)]*\)', '').alias('도로명주소'))
        # sub_add_df.show()

        sub_add_df = sub_add_df.withColumnRenamed('역명', 'STATION_NAME') \
                            .withColumnRenamed('도로명주소', 'ADDRESS')
        # sub_add_df.show()

        # 서울 지하철역 경위도 데이터
        sub_coord_df = get_spark_session().read.csv(cls.path_coord, encoding='UTF-8', header=True)
        sub_coord_df.show()

        sub_coord_df = sub_coord_df.select('역명', 'LON', 'LAT') \
                                    .withColumnRenamed('역명', 'STATION_NAME')
        # sub_coord_df.show()

        subway_coord_add = sub_coord_df.join(sub_add_df, on='STATION_NAME')
        subway_coord_add.show()

        subway_coord_add = subway_coord_add.select('STATION_NAME'
                                                , col('LAT').cast('float').alias('LAT')
                                                , col('LON').cast('float').alias('LON')
                                                , 'ADDRESS')

        subway_coord_add.show()

        # subway_coord_add.printSchema()

        save_data(DataWarehouse, subway_coord_add, 'SUBWAY')
