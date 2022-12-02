import unittest
from datajob.etl.transform.float_pop import FloatPopTransform
from datajob.etl.transform.subway import SubwayAdd
from datajob.etl.transform.transform_loc import Loc


class MTest(unittest.TestCase):

    def test1(self):
        FloatPopTransform.transform()

    def test2(self):
        SubwayAdd.sub_add()

    def test3(self):
        Loc.gu_loc()

if __name__ == "__main__":
    unittest.main()