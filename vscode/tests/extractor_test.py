import unittest
from datajob.etl.extract.extract_weather import WeatherByTimeDate
from datajob.etl.extract.extract_weather_now import WeatherNow

class MTest(unittest.TestCase):

    def test1(self):
        WeatherByTimeDate.extract_data()

    def test2(self):
        WeatherNow.extract_data()

if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()