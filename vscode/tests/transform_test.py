import unittest
from datajob.etl.transform.transform_weather import WeatherTransform
from datajob.etl.transform.transform_torism_rest import TourismTransformRest


class MTest(unittest.TestCase):

    def test1(self):
        WeatherTransform.transform()
    
    def test2(self):
        TourismTransformRest.transform()


if __name__ == "__main__":
    unittest.main()