import unittest
from collections import OrderedDict
from hw3 import compare_vc

class TestStringMethods(unittest.TestCase):
    our_vc = [1, 0, 1, 0]

    def test_clock_gt(self):
        compare_vc()
        self.assertEqual()

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

if __name__ == '__main__':
    unittest.main()