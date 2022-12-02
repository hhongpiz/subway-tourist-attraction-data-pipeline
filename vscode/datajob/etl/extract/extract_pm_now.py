# 패키지 임포트

from urllib.request import urlopen
import requests
from bs4 import BeautifulSoup

class PMnow:
    URL ='https://www.airkorea.or.kr/web/dustForecast?pMENU_NO=113'
    @classmethod
    def extract_data(cls):
        html = urlopen(cls.URL)
        bs_obj = BeautifulSoup(html,'html.parser')
        today_10 = bs_obj.select('#cont_body > dl:nth-child(7) > dd > table > tbody > tr:nth-child(2) > td:nth-child(2)')[0].text
        today_25 = bs_obj.select('#cont_body > dl:nth-child(7) > dd > table > tbody > tr:nth-child(3) > td:nth-child(2)')[0].text
        tomm_10 = bs_obj.select('#cont_body > dl:nth-child(8) > dd > table > tbody > tr:nth-child(2) > td:nth-child(2)')[0].text
        tomm_25 = bs_obj.select('#cont_body > dl:nth-child(8) > dd > table > tbody > tr:nth-child(3) > td:nth-child(2)')[0].text

        

        # pm10 = [today_10]*13+[tomm_10]*26
        # pm25 = [today_25]*13+[tomm_25]*26

    