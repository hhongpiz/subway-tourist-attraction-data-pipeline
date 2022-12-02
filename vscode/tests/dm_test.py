import unittest
from datajob.datamart.sub_pop_weather_21 import SubPopWeather21


class MTest(unittest.TestCase):

    # def test1(self):
    #     CoPopuDensity.save()

    # def test2(self):
    #     CoVaccine.save()

    def test3(self):
        SubPopWeather21.save()


if __name__ == "__main__":
    unittest.main()