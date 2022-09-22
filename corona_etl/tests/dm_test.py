import unittest
from datajob.datamart.co_popu_density import CoPopuDensity
from datajob.datamart.co_vaccine import CoVaccine

class MTest(unittest.TestCase):

    def test1(self):
        CoPopuDensity.save()

    def test2(self):
        CoVaccine.save()



if __name__ == "__main__":
    unittest.main()