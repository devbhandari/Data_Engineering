import unittest
import cap

class TestCap(unittest.TestCase):
    def test_one_word(self):
        text='python'
        result=cap.cap_test(text)
        self.assertEqual(result,'Python')

    def test_two_multi_word(self):
        text='monty python'
        result=cap.cap_test(text)
        self.assertEqual(result,'Monty Python')
if __name__ == '__main__':
    unittest.main()
