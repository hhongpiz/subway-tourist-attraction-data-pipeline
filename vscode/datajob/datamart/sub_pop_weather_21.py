from operator import getitem
from infra.jdbc import DataWarehouse, DataMart, find_data, save_data
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


class SubPopWeather21:

    @classmethod
    def save(cls):
        weather = find_data(DataWarehouse, 'WEATHER')

        float_pop = find_data(DataWarehouse, 'FLOAT_POP')
        float_pop = float_pop.select('S_IDX', 'W_IDX', 'UP_POP', 'DOWN_POP')

        subway = find_data(DataWarehouse, 'SUBWAY')
        subway = subway.select('S_IDX', 'STATION_NAME', 'G_IDX')

        gu_data = find_data(DataWarehouse, 'GU')
        gu_data = gu_data.select('G_IDX', 'GU')

        pm_data = find_data(DataWarehouse, 'PM')
        pm_data = pm_data.select('ITEM_CODE', 'DATA_VALUE', 'W_IDX', 'G_IDX')

        # 1분기 데이터 DM에 save
        # 1분기 미세먼지 데이터
        pm_q1 = pm_data.where((pm_data.W_IDX >= 3577) & (pm_data.W_IDX <= 4735))

        pm10_q1 = pm_q1.where(pm_q1.ITEM_CODE == 8) \
                            .drop('ITEM_CODE')

        pd_pm10_q1 = pm10_q1.to_pandas_on_spark()
        pd_pm10_q1 = pd_pm10_q1.set_index(['W_IDX', 'G_IDX'])

        rev_pd_pm10_q1 = pd_pm10_q1.stack()
        rev_pd_pm10_q1 = rev_pd_pm10_q1.to_dataframe('DATA_VALUE')

        rev_pd_pm10_q1 = rev_pd_pm10_q1.reset_index()

        pm10_q1_df = rev_pd_pm10_q1.to_spark()
        windowSpec = Window.orderBy(col('W_IDX'), col('G_IDX'))
        pm10_q1_df = pm10_q1_df.withColumn('rnum', row_number().over(windowSpec))
        pm10_q1_df = pm10_q1_df.withColumnRenamed('DATA_VALUE', 'PM10') \
                            .drop('level_2')

        # 1분기 초미세먼지 데이터
        pm25_q1 = pm_q1.where(pm_q1.ITEM_CODE == 9) \
                            .drop('ITEM_CODE')

        pd_pm25_q1 = pm25_q1.to_pandas_on_spark()
        pd_pm25_q1 = pd_pm25_q1.set_index(['W_IDX', 'G_IDX'])

        rev_pd_pm25_q1 = pd_pm25_q1.stack()
        rev_pd_pm25_q1 = rev_pd_pm25_q1.to_dataframe('DATA_VALUE')

        rev_pd_pm25_q1 = rev_pd_pm25_q1.reset_index()

        pm25_q1_df = rev_pd_pm25_q1.to_spark()
        windowSpec = Window.orderBy(col('W_IDX'), col('G_IDX'))
        pm25_q1_df = pm25_q1_df.withColumn('rnum', row_number().over(windowSpec))
        pm25_q1_df = pm25_q1_df.withColumnRenamed('DATA_VALUE', 'PM25') \
                            .drop('level_2', 'W_IDX', 'G_IDX')

        pm_q1_df = pm10_q1_df.join(pm25_q1_df, on='rnum').drop('rnum')
        pm_q1_df.show()

        # 자치구 데이터와 결합
        pm_q1_gu = pm_q1_df.join(gu_data, on='G_IDX')
        pm_q1_gu.show()

        # 1분기 날씨데이터와 결합
        weather_q1 = weather.where((weather.DAY >= '2021-01-01') & (weather.DAY <= '2021-03-31'))
        weather_pm_gu_q1 = weather_q1.join(pm_q1_gu, on='W_IDX')
        weather_pm_gu_q1.show()

        # 1분기 유동인구 데이터와 지하철 결합 결합
        float_pop_q1 = float_pop.where((float_pop.W_IDX >= 3577) & (float_pop.W_IDX <= 4735))

        sub_float_q1 = float_pop_q1.join(subway, on='S_IDX')
        sub_float_q1.show()

        # 1분기 [날씨,미세먼지,자치구] 와 [유동인구, 지하철] 결합
        sub_float_weather_q1 = weather_pm_gu_q1.join(sub_float_q1, on=['W_IDX', 'G_IDX'])
        sub_float_weather_q1 = sub_float_weather_q1.drop('W_IDX', 'G_IDX', 'S_IDX')

        sub_float_weather_q1 = sub_float_weather_q1.select(sub_float_weather_q1.DAY.substr(0, 10).alias('DAY').cast('date')
                                                        , 'TIME', 'UP_POP', 'DOWN_POP', 'STATION_NAME', 'GU'
                                                        , 'RAIN', 'HUMN', 'SNOW', 'SKY', 'ONDO', 'WINDD', 'WINDS'
                                                        , 'PM10', 'PM25').orderBy(['DAY', 'TIME', 'STATION_NAME'])

        sub_float_weather_q1.printSchema()
        sub_float_weather_q1.show()

        # save_data(DataMart, sub_float_weather_q1, 'FINAL_TABLE')

        # # 2분기 데이터 DM에 save
        # # 2분기 미세먼지 데이터
        # pm_q2 = pm_data.where((pm_data.W_IDX >= 2394) & (pm_data.W_IDX <= 3576))

        # pm10_q2 = pm_q2.where(pm_q2.ITEM_CODE == 8) \
        #                     .drop('ITEM_CODE')

        # pd_pm10_q2 = pm10_q2.to_pandas_on_spark()
        # pd_pm10_q2 = pd_pm10_q2.set_index(['W_IDX', 'G_IDX'])

        # rev_pd_pm10_q2 = pd_pm10_q2.stack()
        # rev_pd_pm10_q2 = rev_pd_pm10_q2.to_dataframe('DATA_VALUE')

        # rev_pd_pm10_q2 = rev_pd_pm10_q2.reset_index()

        # pm10_q2_df = rev_pd_pm10_q2.to_spark()
        # windowSpec = Window.orderBy(col('W_IDX'), col('G_IDX'))
        # pm10_q2_df = pm10_q2_df.withColumn('rnum', row_number().over(windowSpec))
        # pm10_q2_df = pm10_q2_df.withColumnRenamed('DATA_VALUE', 'PM10') \
        #                     .drop('level_2')

        # # 2분기 초미세먼지 데이터
        # pm25_q2 = pm_q2.where(pm_q2.ITEM_CODE == 9) \
        #                     .drop('ITEM_CODE')

        # pd_pm25_q2 = pm25_q2.to_pandas_on_spark()
        # pd_pm25_q2 = pd_pm25_q2.set_index(['W_IDX', 'G_IDX'])

        # rev_pd_pm25_q2 = pd_pm25_q2.stack()
        # rev_pd_pm25_q2 = rev_pd_pm25_q2.to_dataframe('DATA_VALUE')

        # rev_pd_pm25_q2 = rev_pd_pm25_q2.reset_index()

        # pm25_q2_df = rev_pd_pm25_q2.to_spark()
        # windowSpec = Window.orderBy(col('W_IDX'), col('G_IDX'))
        # pm25_q2_df = pm25_q2_df.withColumn('rnum', row_number().over(windowSpec))
        # pm25_q2_df = pm25_q2_df.withColumnRenamed('DATA_VALUE', 'PM25') \
        #                     .drop('level_2', 'W_IDX', 'G_IDX')

        # pm_q2_df = pm10_q2_df.join(pm25_q2_df, on='rnum').drop('rnum')
        # pm_q2_df.show()

        # # 자치구 데이터와 결합
        # pm_q2_gu = pm_q2_df.join(gu_data, on='G_IDX')
        # pm_q2_gu.show()

        # # 2분기 날씨데이터와 결합
        # weather_q2 = weather.where((weather.DAY >= '2021-04-01') & (weather.DAY <= '2021-06-30'))
        # weather_pm_gu_q2 = weather_q2.join(pm_q2_gu, on='W_IDX')
        # weather_pm_gu_q2.show()

        # # 2분기 유동인구 데이터와 지하철 결합 결합
        # float_pop_q2 = float_pop.where((float_pop.W_IDX >= 2394) & (float_pop.W_IDX <= 3576))

        # sub_float_q2 = float_pop_q2.join(subway, on='S_IDX')
        # sub_float_q2.show()

        # # 2분기 [날씨,미세먼지,자치구] 와 [유동인구, 지하철] 결합
        # sub_float_weather_q2 = weather_pm_gu_q2.join(sub_float_q2, on=['W_IDX', 'G_IDX'])
        # sub_float_weather_q2 = sub_float_weather_q2.drop('W_IDX', 'G_IDX', 'S_IDX').show()

        # sub_float_weather_q2 = sub_float_weather_q2.select(sub_float_weather_q2.DAY.substr(0, 10).alias('DAY').cast('date')
        #                                                 , 'TIME', 'UP_POP', 'DOWN_POP', 'STATION_NAME', 'GU'
        #                                                 , 'RAIN', 'HUMN', 'SNOW', 'SKY', 'ONDO', 'WINDD', 'WINDS'
                                                        # , 'PM10', 'PM25').orderBy(['DAY', 'TIME', 'STATION_NAME'])

        # sub_float_weather_q2.printSchema()
        # sub_float_weather_q2.show()

        # save_data(DataMart, sub_float_weather_q2, 'FINAL_TABLE')

        # # 3분기 데이터 DM에 save
        # # 3분기 미세먼지 데이터
        # pm_q3 = pm_data.where((pm_data.W_IDX >= 1198) & (pm_data.W_IDX <= 2393))

        # pm10_q3 = pm_q3.where(pm_q3.ITEM_CODE == 8) \
        #                     .drop('ITEM_CODE')

        # pd_pm10_q3 = pm10_q3.to_pandas_on_spark()
        # pd_pm10_q3 = pd_pm10_q3.set_index(['W_IDX', 'G_IDX'])

        # rev_pd_pm10_q3 = pd_pm10_q3.stack()
        # rev_pd_pm10_q3 = rev_pd_pm10_q3.to_dataframe('DATA_VALUE')

        # rev_pd_pm10_q3 = rev_pd_pm10_q3.reset_index()

        # pm10_q3_df = rev_pd_pm10_q3.to_spark()
        # windowSpec = Window.orderBy(col('W_IDX'), col('G_IDX'))
        # pm10_q3_df = pm10_q3_df.withColumn('rnum', row_number().over(windowSpec))
        # pm10_q3_df = pm10_q3_df.withColumnRenamed('DATA_VALUE', 'PM10') \
        #                     .drop('level_2')

        # # 3분기 초미세먼지 데이터
        # pm25_q3 = pm_q3.where(pm_q3.ITEM_CODE == 9) \
        #                     .drop('ITEM_CODE')

        # pd_pm25_q3 = pm25_q3.to_pandas_on_spark()
        # pd_pm25_q3 = pd_pm25_q3.set_index(['W_IDX', 'G_IDX'])

        # rev_pd_pm25_q3 = pd_pm25_q3.stack()
        # rev_pd_pm25_q3 = rev_pd_pm25_q3.to_dataframe('DATA_VALUE')

        # rev_pd_pm25_q3 = rev_pd_pm25_q3.reset_index()

        # pm25_q3_df = rev_pd_pm25_q3.to_spark()
        # windowSpec = Window.orderBy(col('W_IDX'), col('G_IDX'))
        # pm25_q3_df = pm25_q3_df.withColumn('rnum', row_number().over(windowSpec))
        # pm25_q3_df = pm25_q3_df.withColumnRenamed('DATA_VALUE', 'PM25') \
        #                     .drop('level_2', 'W_IDX', 'G_IDX')

        # pm_q3_df = pm10_q3_df.join(pm25_q3_df, on='rnum').drop('rnum')
        # pm_q3_df.show()

        # # 자치구 데이터와 결합
        # pm_q3_gu = pm_q3_df.join(gu_data, on='G_IDX')
        # pm_q3_gu.show()

        # # 3분기 날씨데이터와 결합
        # weather_q3 = weather.where((weather.DAY >= '2021-07-01') & (weather.DAY <= '2021-09-30'))
        # weather_pm_gu_q3 = weather_q3.join(pm_q3_gu, on='W_IDX')
        # weather_pm_gu_q3.show()

        # # 3분기 유동인구 데이터와 지하철 결합 결합
        # float_pop_q3 = float_pop.where((float_pop.W_IDX >= 1198) & (float_pop.W_IDX <= 2393))

        # sub_float_q3 = float_pop_q3.join(subway, on='S_IDX')
        # sub_float_q3.show()

        # # 3분기 [날씨,미세먼지,자치구] 와 [유동인구, 지하철] 결합
        # sub_float_weather_q3 = weather_pm_gu_q3.join(sub_float_q3, on=['W_IDX', 'G_IDX'])
        # sub_float_weather_q3 = sub_float_weather_q3.drop('W_IDX', 'G_IDX', 'S_IDX').show()

        # sub_float_weather_q3 = sub_float_weather_q3.select(sub_float_weather_q3.DAY.substr(0, 10).alias('DAY').cast('date')
        #                                                 , 'TIME', 'UP_POP', 'DOWN_POP', 'STATION_NAME', 'GU'
        #                                                 , 'RAIN', 'HUMN', 'SNOW', 'SKY', 'ONDO', 'WINDD', 'WINDS'
                                                        # , 'PM10', 'PM25').orderBy(['DAY', 'TIME', 'STATION_NAME'])

        # sub_float_weather_q3.printSchema()
        # sub_float_weather_q3.show()

        # save_data(DataMart, sub_float_weather_q3, 'FINAL_TABLE')

        # # 4분기 데이터 DM에 save
        # # 4분기 미세먼지 데이터
        # pm_q4 = pm_data.where(pm_data.W_IDX <= 1197)

        # pm10_q4 = pm_q4.where(pm_q4.ITEM_CODE == 8) \
        #                     .drop('ITEM_CODE')

        # pd_pm10_q4 = pm10_q4.to_pandas_on_spark()
        # pd_pm10_q4 = pd_pm10_q4.set_index(['W_IDX', 'G_IDX'])

        # rev_pd_pm10_q4 = pd_pm10_q4.stack()
        # rev_pd_pm10_q4 = rev_pd_pm10_q4.to_dataframe('DATA_VALUE')

        # rev_pd_pm10_q4 = rev_pd_pm10_q4.reset_index()

        # pm10_q4_df = rev_pd_pm10_q4.to_spark()
        # windowSpec = Window.orderBy(col('W_IDX'), col('G_IDX'))
        # pm10_q4_df = pm10_q4_df.withColumn('rnum', row_number().over(windowSpec))
        # pm10_q4_df = pm10_q4_df.withColumnRenamed('DATA_VALUE', 'PM10') \
        #                     .drop('level_2')

        # # 4분기 초미세먼지 데이터
        # pm25_q4 = pm_q4.where(pm_q4.ITEM_CODE == 9) \
        #                     .drop('ITEM_CODE')

        # pd_pm25_q4 = pm25_q4.to_pandas_on_spark()
        # pd_pm25_q4 = pd_pm25_q4.set_index(['W_IDX', 'G_IDX'])

        # rev_pd_pm25_q4 = pd_pm25_q4.stack()
        # rev_pd_pm25_q4 = rev_pd_pm25_q4.to_dataframe('DATA_VALUE')

        # rev_pd_pm25_q4 = rev_pd_pm25_q4.reset_index()

        # pm25_q4_df = rev_pd_pm25_q4.to_spark()
        # windowSpec = Window.orderBy(col('W_IDX'), col('G_IDX'))
        # pm25_q4_df = pm25_q4_df.withColumn('rnum', row_number().over(windowSpec))
        # pm25_q4_df = pm25_q4_df.withColumnRenamed('DATA_VALUE', 'PM25') \
        #                     .drop('level_2', 'W_IDX', 'G_IDX')

        # pm_q4_df = pm10_q4_df.join(pm25_q4_df, on='rnum').drop('rnum')
        # pm_q4_df.show()

        # # 자치구 데이터와 결합
        # pm_q4_gu = pm_q4_df.join(gu_data, on='G_IDX')
        # pm_q4_gu.show()

        # # 4분기 날씨데이터와 결합
        # weather_q4 = weather.where((weather.DAY >= '2021-10-01') & (weather.DAY <= '2021-12-31'))
        # weather_pm_gu_q4 = weather_q4.join(pm_q4_gu, on='W_IDX')
        # weather_pm_gu_q4.show()

        # # 4분기 유동인구 데이터와 지하철 결합 결합
        # float_pop_q4 = float_pop.where(float_pop.W_IDX <= 1197)

        # sub_float_q4 = float_pop_q4.join(subway, on='S_IDX')
        # sub_float_q4.show()

        # # 1분기 [날씨,미세먼지,자치구] 와 [유동인구, 지하철] 결합
        # sub_float_weather_q4 = weather_pm_gu_q4.join(sub_float_q4, on=['W_IDX', 'G_IDX'])
        # sub_float_weather_q4 =sub_float_weather_q4.drop('W_IDX', 'G_IDX', 'S_IDX').show()

        # sub_float_weather_q4 = sub_float_weather_q4.select(sub_float_weather_q4.DAY.substr(0, 10).alias('DAY').cast('date')
        #                                                 , 'TIME', 'UP_POP', 'DOWN_POP', 'STATION_NAME', 'GU'
        #                                                 , 'RAIN', 'HUMN', 'SNOW', 'SKY', 'ONDO', 'WINDD', 'WINDS'
                                                        # , 'PM10', 'PM25').orderBy(['DAY', 'TIME', 'STATION_NAME'])

        # sub_float_weather_q4.printSchema()
        # sub_float_weather_q4.show()

        # save_data(DataMart, sub_float_weather_q4, 'FINAL_TABLE')
