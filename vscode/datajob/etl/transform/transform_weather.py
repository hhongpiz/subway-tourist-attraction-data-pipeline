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
        for i in range(1026,1027): # 294, 1390
            try:
                file_name = '/final_data/weather/weather_' + cal_std_day(i) + '.json'
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
                        ,col('TA').cast('float'),col('RN').cast('float'),col('WS').cast('float'),col('WD').cast('int'),col('HM').cast('int')
                        ,col('PV').cast('float'),col('TD').cast('float'),col('PA').cast('float'),col('PS').cast('float'),col('SS').cast('float')
                        ,col('ICSR').cast('float'),col('DSNW').cast('float'),col('VS').cast('float'),col('TS').cast('float')) \
                        .filter((col('time') >= '10:00') & (col('time') <= '22:00')) \
                        .orderBy(col('time'))
                # weather.show() 
                save_data(DataWarehouse, weather, 'WEATHER')

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