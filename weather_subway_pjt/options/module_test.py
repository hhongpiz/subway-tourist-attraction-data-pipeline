import pandas as pd
import numpy as np
import warnings
import joblib
from sklearn.linear_model import LinearRegression
from datetime import datetime
# warnings.filterwarnings(action='ignore')



# 넘어온 정보를 가중치와 병합, 길이를 맞춤
def pre_predict(wea,gu):
    weather = pd.DataFrame()
    station_W = pd.read_csv('options/dummie_data/station_W.csv',encoding='cp949')

    W = station_W[station_W['GU'] == gu].set_index('STATION_NAME',drop=False)[['W']]
    wea = pd.DataFrame(wea)
    W = np.log1p(W)
    # gu에서 받아온 역의 갯수만큼 날씨변수 생성
    for i in range(len(W)):
        weather = pd.concat([weather,wea],axis=0)
        # gu에서 받아온 역의 갯수만큼 날씨변수 생성
    weather.index = W.index
    feat=weather.columns.drop('ONDO')
    weather[feat]=np.log1p(weather[feat])
    values = pd.concat([weather,W], axis=1)
    values.index = W.index# 날씨와 가중치 컬럼 합병
    return values

# 웹서버에 모델 저장해놓기(csv와 경로 똑같이)
def predict(values):
    lr = joblib.load('options/dummie_data/weather_lr.pkl')
    result = lr.predict(values)
    result = pd.DataFrame(np.expm1(result))
    result.index = values.index
    result.columns = ['result']
    result['result'] = result['result'].astype(int)
    result.sort_values(by='result', ascending=False, inplace=True)
    result.reset_index(drop=False,inplace=True)

    return result

# 0. view.py에서 weather_import() 호출
def weather_import(day,time,gu):
# 3일치를 모두 csv파일로 저장
# csv 로드 후 day가 key
# 1. csv 로드 3일치 예보
    wea = pd.read_csv('options/dummie_data/forecast.csv')# 1. csv 로드 3일치 예보
# 2. 로드된 df에서 오늘/내일/모레 & 시간를 인덱싱 후 가져오기
    wea = wea[['RAIN', 'HUMN', 'SNOW', 'SKY', 'ONDO', 'WINDD', 'WINDS', 'PM10', 'PM25', 'DAY', 'TIME']] # 컬럼 순서에 맞게 정렬
    # wea = wea[['RAIN', 'HUMN', 'SNOW', 'SKY', 'ONDO', 'WINDD', 'WINDS', 'DAY', 'TIME']]
    T=[0 if (int(i.split(':')[0])>19)|(int(i.split(':')[0])<17) else 1 for i in wea['TIME']] # 시간대 컬럼 생성
    wea['TIME'] = pd.to_datetime(wea['TIME']) # 시간정보를 문자열에서 시간변수로 변환
    wea['time'] = wea['TIME'].dt.strftime('%H').astype(int)# 변환된 시간변수로 시간컬럼 생성


    # wea['PM10'] = [32.8]*39 # 미세먼지 컬럼 (임시)
    # wea['PM25'] = [19.2]*39 # 초미세먼지 컬럼 (임시)
    wea['T'] = T # 시간대 컬럼 삽입
    wea = {'today':wea[:13], 'day1':wea[13:26],'day2':wea[26:]} # 3일치에 맞게 컬럼 분할
    weather = wea[day][wea[day]['time'] == int(time)] # 요청받은 날짜, 시간에 맞는 예보데이터 인덱싱
    date = weather['DAY']


    weather = weather[['RAIN', 'HUMN', 'SNOW', 'SKY', 'ONDO', 'WINDD', 'WINDS', 'PM10', 'PM25','T']]
    # weather.drop('time', axis=1, inplace=True) # 인덱싱을 위한 시간변수 삭제
# 3. 예측
    result = predict(pre_predict(weather,gu)).round(0)
    station_W = pd.read_csv('options/dummie_data/station_W.csv', encoding='cp949')
    dumm = station_W.set_index('STATION_NAME')
    result.set_index('STATION_NAME',inplace=True)
    LAT_LON = dumm.loc[result.index][['LAT','LON']]
    result = pd.concat([result,LAT_LON],axis=1).reset_index()
    result.index=result.index+1
    return weather, result, date


# (py파일, 모델파일, 날씨.csv, station_W.csv 같은 폴더에)
# weather,result = weather_import('day1', '13', '종로구')
# print(result)
# print(weather)