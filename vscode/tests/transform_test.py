import unittest
from datajob.etl.transform.transform_weather import WeatherTransform
from datajob.etl.transform.transform_torism_rest import TourismRestTransformt


class MTest(unittest.TestCase):

    def test1(self):
        WeatherTransform.transform()
    
    def test2(self):
        TourismRestTransformt.transform()


if __name__ == "__main__":
    unittest.main()