try:
    import unittest2 as unittest
except ImportError:
    import unittest
import hadoopy_flow
import gevent

# Cheat Sheet (method/test) <http://docs.python.org/library/unittest.html>
#
# assertEqual(a, b)       a == b   
# assertNotEqual(a, b)    a != b    
# assertTrue(x)     bool(x) is True  
# assertFalse(x)    bool(x) is False  
# assertRaises(exc, fun, *args, **kwds) fun(*args, **kwds) raises exc
# assertAlmostEqual(a, b)  round(a-b, 7) == 0         
# assertNotAlmostEqual(a, b)          round(a-b, 7) != 0
# 
# Python 2.7+ (or using unittest2)
#
# assertIs(a, b)  a is b
# assertIsNot(a, b) a is not b
# assertIsNone(x)   x is None
# assertIsNotNone(x)  x is not None
# assertIn(a, b)      a in b
# assertNotIn(a, b)   a not in b
# assertIsInstance(a, b)    isinstance(a, b)
# assertNotIsInstance(a, b) not isinstance(a, b)
# assertRaisesRegexp(exc, re, fun, *args, **kwds) fun(*args, **kwds) raises exc and the message matches re
# assertGreater(a, b)       a > b
# assertGreaterEqual(a, b)  a >= b
# assertLess(a, b)      a < b
# assertLessEqual(a, b) a <= b
# assertRegexpMatches(s, re) regex.search(s)
# assertNotRegexpMatches(s, re)  not regex.search(s)
# assertItemsEqual(a, b)    sorted(a) == sorted(b) and works with unhashable objs
# assertDictContainsSubset(a, b)      all the key/value pairs in a exist in b


def output_trivial():
    gevent.sleep(.1)
    return {'process': 'my proc'}


def output_longer():
    gevent.sleep(2)
    return {'process': 'my proc'}


class Test(unittest.TestCase):

    def setUp(self):
        self.a = hadoopy_flow.Greenlet(output_trivial)
        self.a.start()

    def tearDown(self):
        pass

    def test_name(self):
        a = self.a
        self.assertEqual(hadoopy_flow.LazyReturn(a)['process'], 'my proc')
        self.assertEqual(hadoopy_flow.LazyReturn(a).keys(), ['process'])

    def test_mutate_dict(self):
        a = self.a
        b = hadoopy_flow.LazyReturn(a)
        b['test'] = 1
        self.assertEqual(set(hadoopy_flow.LazyReturn(a).keys()), set(['test', 'process']))

    def test_longer(self):
        hadoopy_flow.Greenlet(output_longer).start()

if __name__ == '__main__':
    unittest.main()
