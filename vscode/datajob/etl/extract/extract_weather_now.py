import json
from infra.logger import get_logger
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client
from infra.spark_session import get_spark_session

class WeatherNow:
    URL = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst'
    SERVICE_KEY = 'lWLaCFJasDo6vpnFordk0ZVBBDk0eu0yL+KUjEF56K+w78w4lsKg7BJANNWeWbL2mzl72Q4LfFyIygL5qCGGkA=='
    FILE_DIR = '/final_data/weather_now/'
    @classmethod
    def extract_data(cls):
        for i in range(1): 
            params = {'ServiceKey':cls.SERVICE_KEY
                ,'pageNo':'1'
                ,'numOfRows':'1000'
                ,'dataType':'JSON'
                ,'base_date':cal_std_day(i) # 예보기준일자 (조회는 +3일까지)
                ,'base_time':'0800' # 예보시간
                ,'nx':'60'
                ,'ny':'127'  # 서울 좌표 60 127
            }

            try:
                res = execute_rest_api('get',cls.URL, {}, params)
                file_name = 'weather_now_' + params['base_date'] + '.json'
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
        get_logger('weather_now',i).error(log_json)

    @classmethod
    def __create_log_dict(cls, params):
        log_dict = {
                    "is_success":"Fail"
                ,   "type":"weather_now"
                ,   "std_day":params['base_date']
                ,   "params":params
            }
        
        return log_dict
