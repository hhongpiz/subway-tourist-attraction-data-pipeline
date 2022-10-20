from pyspark.sql.functions import *
from pyspark.sql import Row
from infra.jdbc import DataWarehouse, save_data
from infra.spark_session import get_spark_session
from infra.util import cal_std_day

class WeatherTransform:
    @classmethod
    def transform(cls):
        for i in range(300,301):
            file_name = '/final_data/weather/weather_' + cal_std_day(i) + '.json'
            tmp = get_spark_session().read.json(file_name, multiLine=True, encoding='utf-8')
            tmp2 = tmp.select('response').first()
            df = get_spark_session().createDataFrame(tmp2)
            tmp3 = df.select('body').first()
            tmp4 = get_spark_session().createDataFrame(tmp3)
            tmp5 = tmp4.select('items').first()
            df2 = get_spark_session().createDataFrame(tmp5).first()
            fin_df = get_spark_session().createDataFrame(df2['item'])

            weather = fin_df.select(substring(col('tm'),1,10).alias('DAY')
                    ,substring(col('tm'),12,17).alias('TIME')
                    ,col('TA'),col('RN'),col('WS'),col('WD'),col('HM')
                    ,col('PV'),col('TD'),col('PA'),col('PS'),col('SS')
                    ,col('ICSR'),col('DSNW'),col('VS'),col('TS')) \
                    .filter((col('time') >= '10:00') & (col('time') <= '22:00'))
            # weather.show() 
            save_data(DataWarehouse, weather, 'WEATHER')