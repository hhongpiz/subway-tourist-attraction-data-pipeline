import unittest
from datajob.etl.transform.transform_weather import WeatherTransform
from datajob.etl.transform.transform_torism_rest import TourismRestTransform
from datajob.etl.transform.transform_weather_now import WeatherNowTransform


class MTest(unittest.TestCase):

    def test1(self):
        WeatherTransform.transform()
    
    def test2(self):
        TourismRestTransform.transform()

    def test3(self):
        WeatherNowTransform.transform()



if __name__ == "__main__":
    unittest.main()