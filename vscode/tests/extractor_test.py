import unittest
from vscode.datajob.etl.extract.subway import SubwayExtractor


class MTest(unittest.TestCase):

    def test1(self):
        SubwayExtractor.extract_data()



if __name__ == "__main__":
    """ This is executed when run from the command line """
    unittest.main()
