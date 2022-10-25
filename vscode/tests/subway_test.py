import unittest
from datajob.etl.transform.float_pop import FloatPopTransform
from datajob.etl.transform.subway import SubwayAdd
from datajob.etl.transform.tour_sub import TourSub



class MTest(unittest.TestCase):

    def test1(self):
        FloatPopTransform.transform()

    def test2(self):
        SubwayAdd.sub_add()

    def test3(self):
        TourSub.tour_sub()

if __name__ == "__main__":
    unittest.main()