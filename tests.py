import unittest

from tasks import DataAnalyzingTask


class TestOutput(unittest.TestCase):
    def test_city(self):
        dant = DataAnalyzingTask()
        dant.collect_data()
        city = dant.choose_best()
        self.assertEqual(city, 'BEIJING')


if __name__ == '__main__':
    unittest.main()
