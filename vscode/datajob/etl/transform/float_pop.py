from infra.hdfs_client import get_client
from pyspark.sql.functions import monotonically_increasing_id
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from datetime import datetime, timedelta

class FloatPopTransform:
    path = '/final_data/subway/subway_2021.csv'

    @classmethod
    def transform(cls):
        sub_df = get_spark_session().read.csv(cls.path, encoding='UTF-8', header=True)
        sub_df.show()

        sub_df = sub_df.select('*').where(sub_df.합계 != 0)
        sub_df = sub_df.select('*').where((sub_df.역번호 != 2565) & (sub_df.역번호 != 2828))
        sub_df = sub_df.drop('연번', '역번호', '합계')
        # sub_df.show()

        sub_on_df = sub_df.select('*').where(sub_df.구분 == '승차')
        sub_on_df = sub_on_df.drop('구분')
        sub_on_df.show()

        pd_subway_on = sub_on_df.to_pandas_on_spark()
        # print(pd_subway_on)
        pd_subway_on = pd_subway_on.set_index(['날짜', '역명'])
        print(pd_subway_on)

        pd_rev_subway_on = pd_subway_on.stack()
        # print(pd_rev_subway_on)

        pd_rev_subway_on = pd_rev_subway_on.to_dataframe('승차인구')
        # print(pd_rev_subway_on)

        pd_rev_subway_on = pd_rev_subway_on.reset_index()
        # print(pd_rev_subway_on)
        pd_rev_subway_on = pd_rev_subway_on.rename(columns={'level_2': '시간'})

        subway_on = pd_rev_subway_on.to_spark()
        subway_on = subway_on.withColumn('idm', monotonically_increasing_id()) \
                            .withColumnRenamed('날짜', 'DAY')

        subway_on.show()

        sub_off_df = sub_df.select('*').where(sub_df.구분 == '하차')
        sub_off_df = sub_off_df.drop('구분')
        sub_off_df.show()

        pd_subway_off = sub_off_df.to_pandas_on_spark()
        # print(pd_subway_on)
        pd_subway_off = pd_subway_off.set_index(['날짜', '역명'])
        # print(pd_subway_on)

        pd_rev_subway_off = pd_subway_off.stack()
        # print(pd_rev_subway_on)

        pd_rev_subway_off = pd_rev_subway_off.to_dataframe('하차인구')
        # print(pd_rev_subway_off)

        pd_rev_subway_off = pd_rev_subway_off.reset_index()
        # print(pd_rev_subway_off)
        pd_rev_subway_off = pd_rev_subway_off.rename(columns={'level_2': '시간2'})

        subway_off = pd_rev_subway_off.to_spark()
        subway_off = subway_off.withColumn('idm', monotonically_increasing_id()) \
                                .withColumnRenamed('날짜', 'DATE_DOWN')
        subway_off = subway_off.drop('역명')
        subway_off.show()

        subway_onoff_df = subway_on.join(subway_off, on='idm')
        # subway_onoff_df = subway_onoff_df.drop('시간2')
        subway_onoff_df.show()

        dump_df = subway_onoff_df.withColumnRenamed('역명', 'STATION_NAME') \
                                .withColumnRenamed('승차인구', 'UP_POP') \
                                .withColumnRenamed('하차인구', 'DOWN_POP') \
                                .withColumnRenamed('시간', 'TIME')
        dump_df = dump_df.drop('idm', '시간2', 'DATE_DOWN')
        dump_df.show()


        # @classmethod
        # def load_weather():
        #     weather_df = find_data(DataWarehouse, 'WEATHER')
        #     weather_idx_df = weather_df.select('W_IDX')
        #     weather_idx_df.show()

        #     subway_pop = dump_df.join(weather_df, on='')
        #     subway_pop.show()

            # save_data(DataWarehouse, subway_pop, 'FLOAT_POP')


    # def cal_std_year(before_year):
    #     x = datetime.now() - relativedelta(before_year)
    #     year = x.year
    #     return str(year)
