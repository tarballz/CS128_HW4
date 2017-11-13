import unittest
from collections import OrderedDict
from hw3 import compare_vc

class TestStringMethods(unittest.TestCase):
    
    def test_cp_gt_vc(self):
        # cp > vc
        vc = [2, 2, 4, 5]
        cp = "2.3.4.7"
        out = compare_vc(vc, cp)
        print(out)
        self.assertEqual(out, 
            [(True, False), (True, True), (True, False), (True, True)])

    def test_cp_lt_vc(self):
        # cp < vc
        vc = [2, 2, 4, 5]
        cp = "1.2.4.5"
        out = compare_vc(vc, cp)
        print(out)
        self.assertEqual(out, 
            [(True, True), (True, False), (True, False), (True, False)])

    def test_cp_vc_concurrent(self):
        # concurrent events
        vc = [2, 2, 4, 5]
        cp = "2.1.5.7"
        out = compare_vc(vc, cp)
        print(out)
        self.assertEqual(out, 
            [(True, False), (True, True), (False, False), (False, False)])


if __name__ == '__main__':
    unittest.main()