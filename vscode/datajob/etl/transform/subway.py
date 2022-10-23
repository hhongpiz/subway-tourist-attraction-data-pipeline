from infra.spark_session import get_spark_session
from infra.jdbc import DataWarehouse, find_data, save_data

class SubwayAdd:

    path = '/final_data/subway/subway_add.csv'

    @classmethod
    def sub_add(cls):
        sub_add_df = get_spark_session().read.csv(cls.path, encoding='UTF-8', header=True)
        sub_add_df.show()

        sub_add_df = sub_add_df.select('역명', '도로명주소')
        sub_add_df.show()

        sub_add_df = sub_add_df.withColumnRenamed('역명', 'STATION_NAME') \
                            .withColumnRenamed('도로명주소', 'ADDRESS')
        sub_add_df.show()

        # save_data(DataWarehouse, sub_add_df, 'SUBWAY')
