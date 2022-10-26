import unittest
from datajob.etl.transform.float_pop import FloatPopTransform
from datajob.etl.transform.subway import SubwayAdd


class MTest(unittest.TestCase):

    def test1(self):
        FloatPopTransform.transform()

    def test2(self):
        SubwayAdd.sub_add()

if __name__ == "__main__":
    unittest.main()