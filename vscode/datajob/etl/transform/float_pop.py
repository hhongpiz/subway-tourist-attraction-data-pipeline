from pyspark.sql.functions import col, regexp_replace, row_number
from infra.jdbc import DataWarehouse, find_data, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_year
from pyspark.sql.window import Window


class FloatPopTransform:

    @classmethod
    def transform(cls):
        for i in range(1,4):
            path = '/final_data/subway/subway_' + cal_std_year(i) + '.csv'
            sub_df = get_spark_session().read.csv(path, encoding='UTF-8', header=True)
            # sub_df.show()

            # 승하차 데이터 합계가 0인 값 및 하남시청역, 남위례역 제거
            sub_df = sub_df.select('*').where(sub_df.합계 != 0)
            sub_df = sub_df.select('*').where((sub_df.역번호 != 2565)
                                            & (sub_df.역번호 != 2828)
                                            & (sub_df.역번호 != 2649)
                                            & (sub_df.역번호 != 2564)
                                            & (sub_df.역번호 != 2563)
                                            & (sub_df.역번호 != 2562)
                                            & (sub_df.역번호 != 2566))
            sub_df = sub_df.drop('연번', '역번호', '합계')
            sub_df.show()

            # 승차 데이터
            sub_on_df = sub_df.select('*').where(sub_df.구분 == '승차')
            # sub_on_df = sub_on_df.withColumn('idm', monotonically_increasing_id())
            sub_on_df = sub_on_df.drop('구분')
            # sub_on_df.show()

            pd_subway_on = sub_on_df.to_pandas_on_spark()
            pd_subway_on = pd_subway_on.set_index(['날짜', '역명'])
            # print(pd_subway_on)

            pd_rev_subway_on = pd_subway_on.stack()

            pd_rev_subway_on = pd_rev_subway_on.to_dataframe('승차인구')

            pd_rev_subway_on = pd_rev_subway_on.reset_index()
            pd_rev_subway_on = pd_rev_subway_on.rename(columns={'level_2': '시간u'})

            # 순차적으로 증가하는 ID값 부여
            subway_on = pd_rev_subway_on.to_spark()
            windowSpec = Window.orderBy(col('날짜'), col('시간u'))
            subway_on = subway_on.withColumn('rnum', row_number().over(windowSpec))
            subway_on = subway_on.withColumnRenamed('날짜', 'DAY_u') \
                                .withColumnRenamed('역명', 'STATION_NAME_u')

            # subway_on = subway_on.to_pandas_on_spark()
            # subway_on = subway_on[:20000]
            # subway_on.to_excel('subway_on_test2.xlsx')


            # 하차 데이터
            sub_off_df = sub_df.select('*').where(sub_df.구분 == '하차')
            sub_off_df = sub_off_df.drop('구분')
            # sub_off_df.show()

            pd_subway_off = sub_off_df.to_pandas_on_spark()
            pd_subway_off = pd_subway_off.set_index(['날짜', '역명'])

            pd_rev_subway_off = pd_subway_off.stack()

            pd_rev_subway_off = pd_rev_subway_off.to_dataframe('하차인구')

            pd_rev_subway_off = pd_rev_subway_off.reset_index()
            pd_rev_subway_off = pd_rev_subway_off.rename(columns={'level_2': '시간d'})

            # 순차적으로 증가하는 ID값 부여
            subway_off = pd_rev_subway_off.to_spark()
            windowSpec = Window.orderBy(col('날짜'), col('시간d'))
            subway_off = subway_off.withColumn('rnum', row_number().over(windowSpec))
            subway_off = subway_off.withColumnRenamed('날짜', 'DAY_d') \
                                    .withColumnRenamed('역명', 'STATION_NAME_d')
            # subway_off.show()
            # subway_off = subway_off.to_pandas_on_spark()
            # subway_off = subway_off[:20000]
            # subway_off.to_excel('subway_on_test3.xlsx')

            # idm 값으로 승하차 df join
            subway_onoff_df = subway_on.join(subway_off, on='rnum')
            subway_onoff_df = subway_onoff_df.drop('DAY_u', 'STATION_NAME_u', '시간u')
            # subway_onoff_df.show()

            # 컬럼명 수정 및 SUBWAY 테이블의 역명에 괄호가 없으므로 괄호 및 괄호 안 문자 제거
            dump_df = subway_onoff_df.withColumnRenamed('STATION_NAME_d', 'STATION_NAME') \
                                    .withColumnRenamed('승차인구', 'UP_POP') \
                                    .withColumnRenamed('하차인구', 'DOWN_POP') \
                                    .withColumnRenamed('시간d', 'TIME') \
                                    .withColumnRenamed('DAY_d', 'DAY')
            dump_df.show()
            dump_df = dump_df.select('UP_POP', 'DOWN_POP', 'TIME', 'DAY'
                                    , regexp_replace(col('STATION_NAME'), r'\([^)]*\)', '') \
                                .alias('STATION_NAME'))

            # 날씨 데이터의 W_IDX와 join을 위해 WEATHER 테이블 가져옴
            weather_df = find_data(DataWarehouse, 'WEATHER')
            weather_idx_df = weather_df.select('W_IDX', 'DAY', 'TIME')
            # weather_idx_df.show()

            subway_pop = weather_idx_df.join(dump_df, on=['DAY', 'TIME'])
            subway_pop = subway_pop.drop('DAY', 'TIME')
            subway_pop.show()

            # 지하철 데이터의 S_IDX와 join을 위해 SUNWAY 테이블 가져옴(하남시청역, 남위례역 제거)
            subway_df = find_data(DataWarehouse, 'SUBWAY')
            subway_df = subway_df.select('*') \
                                    .where((subway_df.STATION_NAME != '하남시청')
                                        & (subway_df.STATION_NAME != '남위례')
                                        & (subway_df.STATION_NAME != '신내')
                                        & (subway_df.STATION_NAME != '하남풍산')
                                        & (subway_df.STATION_NAME != '미사')
                                        & (subway_df.STATION_NAME != '강일')
                                        & (subway_df.STATION_NAME != '하남검단산'))

            float_pop = subway_df.join(subway_pop, on='STATION_NAME') \
                                    .drop('LON', 'LAT', 'ADDRESS')
            # float_pop.show()

            float_pop.createOrReplaceTempView("float_pop")
            float_pop = get_spark_session().sql(""" SELECT S_IDX, W_IDX, SUM(UP_POP), SUM(DOWN_POP)
                                        FROM FLOAT_POP GROUP BY S_IDX, W_IDX """)

            float_pop = float_pop.withColumnRenamed('sum(UP_POP)', 'UP_POP') \
                                    .withColumnRenamed('sum(DOWN_POP)', 'DOWN_POP')
            float_pop.show()
            # float_pop = float_pop.groupby(col('S_IDX'), col('W_IDX')) \
            #                         .agg(sum(col('UP_POP')).alias('UP_POP')
            #                             , sum(col('DOWN_POP')).alias('DOWN_POP'))
            # float_pop.printSchema()

            # 최종 완성 데이터 FLOAT_POP 테이블에 저장
            save_data(DataWarehouse, float_pop, 'FLOAT_POP')
