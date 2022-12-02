import sys

from datajob.etl.extract.extract_weather_now import WeatherNow
from datajob.etl.transform.transform_weather_now import WeatherNowTransform


def transfrom_execute():
    WeatherNowTransform.transform()


def main():
    works = {
        'extract':{
            'extract_weather_now': WeatherNow.extract_data
        }
        ,'transform':{
            'execute':transfrom_execute
            ,'transform_weather_now':WeatherNowTransform.transform
        }
    }
    return works
    
works = main()

if __name__ == "__main__":
    # python3 main.py arg1 arg2
    # 값을 받을 인자는 2개임
    # 인자1 : 작업(extract, transform, datamart)
    # 인자2 : 저장할 위치(테이블)
    # ex) python3 main.py extract corona_api

    args = sys.argv
    
    if len(args) != 3:
        raise Exception('2개의 전달인자가 필요합니다')
    
    if args[1] not in works.keys():
        raise Exception('첫번째 전달인자가 이상함 >> ' + str(works.keys()))

    if args[2] not in works[args[1]].keys():
        raise Exception('두번째 전달인자가 이상함 >> ' + str(works[args[1]].keys()))

    work = works[args[1]][args[2]]
    work()  # 함수객체를 이용해 함수 호출
