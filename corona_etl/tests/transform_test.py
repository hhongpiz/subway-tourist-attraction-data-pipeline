import unittest
from datajob.etl.transform.corona_patient import CoronaPatientTransformer
from datajob.etl.transform.corona_vaccine import CoronaVaccineTransformer
from datajob.etl.transform.loc import LocTransformer

class MTest(unittest.TestCase):

    def test1(self):
        CoronaPatientTransformer.transform()

    def test2(self):
        CoronaVaccineTransformer.transform()

    def test3(self):
        LocTransformer.transform()

if __name__ == "__main__":
    unittest.main()