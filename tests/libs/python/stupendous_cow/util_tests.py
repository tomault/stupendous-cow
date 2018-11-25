from stupendous_cow.util import *
import unittest

class NormalizationTests(unittest.TestCase):
    def test_compress_whitespace(self):
        self.assertEqual('abc def ghi ', compress_whitespace('abc   def ghi  '))

    def test_strip_punct(self):
        self.assertEqual('Dont talk to your coworkers Fred',
                         strip_punct("Don't talk to your co-workers, Fred."))

    def test_normalize_title(self):
        self.assertEqual('kmeans clustering algorithms',
                         normalize_title(' k-Means  Clustering Algorithms.  '))

if __name__ == '__main__':
    unittest.main()
