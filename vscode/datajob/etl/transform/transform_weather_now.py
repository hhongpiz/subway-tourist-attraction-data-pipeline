import json
import pyspark.pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day, cal_std_day_to_date
from infra.logger import get_logger

class WeatherNowTransform:
    @classmethod
    def transform(cls):
        for i in [1]:
            try:
                file_name = '/final_data/weather_now/weather_now_'+ cal_std_day(i) +'.json'
                today = cls.json_to_df(file_name)
                
                # date, time 컬럼생성
                date, time = cls.create_date_time_col()

                # 판다스로 전치            
                pd_today = today.to_pandas_on_spark()
                pd_today = pd_today.set_index(['fcstDate','fcstTime','category'])
                pd_today = pd_today.stack()
                pd_today = pd_today.unstack(2)
                pd_today['TIME'] = time
                pd_today['DAY'] = date
                
                
                # weather 테이블 생성
                weather = pd_today.to_spark()

                weather = weather.select(col('DAY').cast(DateType())
                            ,col('TIME')
                            ,when(col("PCP") == '강수없음', 0).alias("RAIN").cast("float")
                            ,col("REH").alias("HUMN").cast("int")    
                            ,when(col("SNO") == '적설없음', 0).alias("SNOW").cast("float")
                            ,col("SKY").cast("int")    
                            ,col("TMP").alias("ONDO").cast("float")
                            ,floor((col("VEC") + 22.5 * 0.5) / 22.5).alias("WINDD").cast("int")                        
                            ,col("WSD").alias("WINDS").cast("float"))
      

                weather.show()

                save_data(DataWarehouse, weather, 'WEATHER')

            except Exception as e:
                log_dict = cls.__create_log_dict()
                cls.__dump_log(log_dict, e, i)


    @classmethod
    def create_date_time_col(cls):
        date = []
        while len(date) < 13:
            date.append(cal_std_day_to_date(1))
            continue
        while len(date) < 26:
            date.append(cal_std_day_to_date(0))
            continue
        while len(date) < 39:    
            date.append(cal_std_day_to_date(-1))
            continue

        time = [str(t)+':00' for t in range(10,23)]
        time = time*3
        return date,time

    @classmethod
    def json_to_df(cls, file_name):
        tmp = get_spark_session().read.json(file_name, multiLine=True, encoding='utf-8')
        tmp2 = tmp.select('response').first()
        df = get_spark_session().createDataFrame(tmp2)
        tmp3 = df.select('body').first()
        tmp4 = get_spark_session().createDataFrame(tmp3)
        tmp5 = tmp4.select('items').first()
        df2 = get_spark_session().createDataFrame(tmp5).first()
        today = get_spark_session().createDataFrame(df2['item'])

        today = today.select('fcstDate','fcstTime','category','fcstValue') \
                            .filter((col('fcstTime')>='1000') & (col('fcstTime')<='2200'))
                    
        return today

    # 로그 dump
    @classmethod
    def __dump_log(cls, log_dict, e, i):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        print(log_dict['err_msg'])
        get_logger('weather_new',i).error(log_json)

    # 로그데이터 생성
    @classmethod
    def __create_log_dict(cls):
        log_dict = {
                "is_success": "Fail",
                "type": "weather_new"
            }
        return log_dict   

