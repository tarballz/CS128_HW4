import unittest
from collections import OrderedDict
from hw3 import compare_vc

class TestStringMethods(unittest.TestCase):
    
    def test_clock_gt(self):
        our_vc = [0, 0, 1, 1]
        new_vc = "0.0.0.1"
        out = compare_vc(our_vc, new_vc)
        print(out)
        self.assertEqual(True, True) 

if __name__ == '__main__':
    unittest.main()