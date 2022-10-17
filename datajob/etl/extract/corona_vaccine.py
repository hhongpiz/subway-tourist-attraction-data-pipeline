from datetime import datetime
import json
from bs4 import BeautifulSoup
from infra.util import cal_std_day, execute_rest_api
from infra.hdfs_client import get_client


class CoronaVaccineExtractor:

    FILE_DIR = '/corona_data/vaccine/'
    FILE_NAME = 'corona_vaccine_' + cal_std_day(1) + '.json'
    URL = 'https://ncv.kdca.go.kr/mainStatus.es?mid=a11702000000'

    @classmethod
    def extract_data(cls):
        response_txt = execute_rest_api('get', cls.URL, {}, {})
        trs = cls.__parse_vaccine_htmltag(response_txt)
        data = cls.__create_vaccine_json(trs)
        cls.__dump_to_hdfs(data)

    @classmethod
    def __dump_to_hdfs(cls, data):
        res = cls.__create_json_data(data)
        get_client().write(cls.FILE_DIR+cls.FILE_NAME, json.dumps(res, ensure_ascii=False), overwrite=True, encoding='utf-8')

    @classmethod
    def __create_vaccine_json(cls, trs):
        cols = ['loc', 'v1', 'v2', 'v3', 'v4']
        data = []

        for idx, tr in enumerate(trs):
            if idx == 0:
                continue
            rows = cls.__generate_data(tr)
            tmp = dict(zip(cols, rows))
            data.append(tmp)
        return data

    @classmethod
    def __parse_vaccine_htmltag(cls, response_txt):
        soup = BeautifulSoup(response_txt, 'html.parser')
        trs = soup.select('#content > div.data_table.tbl_scrl_t > table > tbody > tr')
        return trs

    @classmethod
    def __create_json_data(cls, data):
        res = {
            'meta': {
                'desc': '지역별 코로나 예방접종 인구 현황',
                'cols': {'loc': '지역', 'v1': '1차접종자수', 'v2': '2차접종자수', 'v3': '3차접종자수', 'v4': '4차접종자수'},
                'std_day': cal_std_day(1)
            },
            'data': data
        }
        return res

    @classmethod
    def __generate_data(cls, tr):
        th = tr.select('th')
        tds = tr.select('td')
        rows = []
        rows.append(th[0].text.replace(' ', '').replace('\r\n', ''))
            
        for idx, td in enumerate(tds):
            if idx % 2 == 0:
                continue            
            rows.append(td.text.replace(' ', '').replace('\r\n', '').replace(',' , ''))
        return rows
