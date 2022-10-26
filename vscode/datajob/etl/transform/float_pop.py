from infra.hdfs_client import get_client
from pyspark.sql.functions import monotonically_increasing_id, col, regexp_replace
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_year


class FloatPopTransform:

    @classmethod
    def transform(cls):
        for i in range(1,2):
            path = '/final_data/subway/subway_' + cal_std_year(i) + '.csv'
            sub_df = get_spark_session().read.csv(path, encoding='UTF-8', header=True)
            sub_df.show()

            # 승하차 데이터 합계가 0인 값 및 하남시청역, 남위례역 제거
            sub_df = sub_df.select('*').where(sub_df.합계 != 0)
            sub_df = sub_df.select('*').where((sub_df.역번호 != 2565) & (sub_df.역번호 != 2828))
            sub_df = sub_df.drop('연번', '역번호', '합계')

            sub_on_df = sub_df.select('*').where(sub_df.구분 == '승차')
            sub_on_df = sub_on_df.drop('구분')

            pd_subway_on = sub_on_df.to_pandas_on_spark()

            pd_subway_on = pd_subway_on.set_index(['날짜', '역명'])
            # print(pd_subway_on)

            pd_rev_subway_on = pd_subway_on.stack()

            pd_rev_subway_on = pd_rev_subway_on.to_dataframe('승차인구')

            pd_rev_subway_on = pd_rev_subway_on.reset_index()

            pd_rev_subway_on = pd_rev_subway_on.rename(columns={'level_2': '시간'})

            # 순차적으로 증가하는 ID값 부여
            subway_on = pd_rev_subway_on.to_spark()
            subway_on = subway_on.withColumn('idm', monotonically_increasing_id()) \
                                .withColumnRenamed('날짜', 'DAY')

            # subway_on.show()

            sub_off_df = sub_df.select('*').where(sub_df.구분 == '하차')
            sub_off_df = sub_off_df.drop('구분')

            pd_subway_off = sub_off_df.to_pandas_on_spark()
            pd_subway_off = pd_subway_off.set_index(['날짜', '역명'])

            pd_rev_subway_off = pd_subway_off.stack()

            pd_rev_subway_off = pd_rev_subway_off.to_dataframe('하차인구')

            pd_rev_subway_off = pd_rev_subway_off.reset_index()
            pd_rev_subway_off = pd_rev_subway_off.rename(columns={'level_2': '시간2'})

            # 순차적으로 증가하는 ID값 부여
            subway_off = pd_rev_subway_off.to_spark()
            subway_off = subway_off.withColumn('idm', monotonically_increasing_id()) \
                                    .withColumnRenamed('날짜', 'DATE_DOWN')
            subway_off = subway_off.drop('역명')
            # subway_off.show()

            # idm 값으로 승하차 df join
            subway_onoff_df = subway_on.join(subway_off, on='idm')
            # subway_onoff_df = subway_onoff_df.drop('시간2')
            subway_onoff_df.show()

            # 컬럼명 수정 및 SUBWAY 테이블의 역명에 괄호가 없으므로 괄호 및 괄호 안 문자 제거
            dump_df = subway_onoff_df.withColumnRenamed('역명', 'STATION_NAME') \
                                    .withColumnRenamed('승차인구', 'UP_POP') \
                                    .withColumnRenamed('하차인구', 'DOWN_POP') \
                                    .withColumnRenamed('시간', 'TIME')
            dump_df = dump_df.drop('idm', '시간2', 'DATE_DOWN')
            dump_df = dump_df.select('UP_POP', 'DOWN_POP', 'TIME', 'DAY'
                                    , regexp_replace(col('STATION_NAME'), r'\([^)]*\)', '') \
                                .alias('STATION_NAME'))
            dump_df.show()

            # 날씨 데이터의 W_IDX와 join을 위해 WEATHER 테이블 가져옴
            weather_df = find_data(DataWarehouse, 'WEATHER')
            weather_idx_df = weather_df.select('W_IDX', 'DAY', 'TIME')
            weather_idx_df.show()

            subway_pop = weather_idx_df.join(dump_df, on=['DAY', 'TIME'])
            # subway_pop = subway_pop.drop('DAY', 'TIME')
            subway_pop.show()

            # 지하철 데이터의 S_IDX와 join을 위해 SUNWAY 테이블 가져옴(하남시청역, 남위례역 제거)
            subway_df = find_data(DataWarehouse, 'SUBWAY')
            subway_df = subway_df.select('*') \
                                    .where((subway_df.STATION_NAME != '하남시청') & (subway_df.STATION_NAME != '남위례'))

            float_pop = subway_df.join(subway_pop, on='STATION_NAME') \
                                    .drop('STATION_NAME', 'LON', 'LAT', 'ADDRESS')
            float_pop.show()
            float_pop = float_pop.groupby(col('S_IDX'), col('W_IDX')) \
                                    .agg(sum(col('UP_POP')).alias('UP_POP')
                                        , sum(col('DOWN_POP')).alias('DOWN_POP'))
            float_pop.show()
            # float_pop.printSchema()

            # 최종 완성 데이터 FLOAT_POP 테이블에 저장
            # save_data(DataWarehouse, float_pop, 'FLOAT_POP')
