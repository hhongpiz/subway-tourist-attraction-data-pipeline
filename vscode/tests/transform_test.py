import unittest
from datajob.etl.transform.transform_weather import WeatherTransform


class MTest(unittest.TestCase):

    def test1(self):
        WeatherTransform.transform()


if __name__ == "__main__":
    unittest.main()