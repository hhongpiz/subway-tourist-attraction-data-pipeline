import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day
from infra.logger import get_logger

class WeatherTransform:
    @classmethod
    def transform(cls):
        list = ['20190929'] #,'20191212','20191215','20200609','20200615','20200616','20200617','20200618','20200619','20200628','20200714','20200715','20200720','20200725','20200727','20200731','20200807','20200816','20200821','20200823','20200824','20200826','20210107']
        for i in list: # 298, 1394
            try:
                file_name = '/final_data/weather/weather_'+ i +'.json'

                tmp = get_spark_session().read.json(file_name, multiLine=True, encoding='utf-8')
                tmp2 = tmp.select('response').first()
                df = get_spark_session().createDataFrame(tmp2)
                tmp3 = df.select('body').first()
                tmp4 = get_spark_session().createDataFrame(tmp3)
                tmp5 = tmp4.select('items').first()
                df2 = get_spark_session().createDataFrame(tmp5).first()
                fin_df = get_spark_session().createDataFrame(df2['item'])

                weather = fin_df.select(substring(col('tm'),1,10).alias('DAY').cast(DateType())
                                        ,substring(col('tm'),12,17).alias('TIME')
                                        ,col('rn').cast('float')
                                        ,col('hm').cast('int')
                                        ,col('hr3Fhsc').cast('float')
                                        ,col('dc10Tca').cast('float')
                                        ,col('ta').cast('float')
                                        ,col('wd').cast('int')
                                        ,col('ws').cast('float')) \
                                        .filter((col('time') >= '10:00') & (col('time') <= '22:00')) \
                                        .orderBy(col('time'))

                weather.createOrReplaceTempView("weather")
                weather = get_spark_session().sql("""select DAY, TIME
                                                    ,nvl(rn, 0) as RAIN
                                                    ,hm as HUMN
                                                    ,case when hr3Fhsc is null then 0 else round((hr3Fhsc/3),1) end as SNOW
                                                    ,case when dc10Tca <= 5 then 1
                                                            when dc10Tca <= 8 then 3 else 4 end as SKY
                                                    ,ta as ONDO
                                                    ,case when wd is null then 0 else floor((wd + 22.5 * 0.5) / 22.5) end as WINDD
                                                    ,nvl(ws,0) as WINDS from weather""") 
                weather.show()

                # save_data(DataWarehouse, weather, 'WEATHER')

            except Exception as e:
                log_dict = cls.__create_log_dict()
                cls.__dump_log(log_dict, e, i)

    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e, i):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('weather',i).error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls):
        log_dict = {
                "is_success": "Fail",
                "type": "weather"
            }
        return log_dict   