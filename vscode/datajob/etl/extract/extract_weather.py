import json
from infra.logger import get_logger
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
from infra.spark_session import get_spark_session

class WeatherByTimeDate:
    URL = 'http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList'
    SERVICE_KEY = 'lWLaCFJasDo6vpnFordk0ZVBBDk0eu0yL+KUjEF56K+w78w4lsKg7BJANNWeWbL2mzl72Q4LfFyIygL5qCGGkA=='
    FILE_DIR = '/final_data/weather/'

    @classmethod
    def extract_data(cls):
        for i in range(290,660): # 294, 660 (2021)
            params = {'ServiceKey':cls.SERVICE_KEY
                ,'numOfRows':'30'
                ,'dataType':'JSON'
                ,'dataCd':'ASOS'
                ,'dateCd':'HR'
                ,'startDt':cal_std_day(i)
                ,'startHh':'00'
                ,'endDt':cal_std_day(i)
                ,'endHh':'23'
                ,'stnIds':'108' # 서울지점코드 108
            }

            try:
                res = execute_rest_api('get',cls.URL, {}, params)
                file_name = 'weather_' + params['startDt'] + '.json'
                cls.__upload_to_hdfs(file_name, res)
                
            except Exception as e:
                log_dict = cls.__create_log_dict(params)
                cls.__dump_log(log_dict, e, i)
                raise e

    @classmethod
    def __upload_to_hdfs(cls, file_name, res):
        get_client().write(cls.FILE_DIR+file_name, res, encoding='utf-8', overwrite=True)

    @classmethod
    def __dump_log(cls, log_dict, e, i):
        log_dict['err_msg'] = e.__str__()
        log_json = json.dumps(log_dict, ensure_ascii=False)
        get_logger('weather',i).error(log_json)

    @classmethod
    def __create_log_dict(cls, params):
        log_dict = {
                    "is_success":"Fail"
                ,   "type":"weather"
                ,   "std_day":params['startDt']
                ,   "params":params
            }
        
        return log_dict

