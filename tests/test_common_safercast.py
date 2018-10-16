import unittest
from mrtarget.common.safercast import SaferInt, SaferBool, SaferFloat


class DataStructureTests(unittest.TestCase):
    def test_safer_int(self):
        fallback_value = 0
        to_int_withfb = SaferInt(with_fallback=fallback_value)
        to_int = SaferInt(with_fallback=None)

        self.assertRaises(ValueError, to_int, "no_number")
        self.assertEqual(to_int_withfb("no_number"), fallback_value, "Failed to fallback an exception")
        self.assertEqual(to_int("1"), 1, "Failed to cast to 1")

    def test_safer_float(self):
        fallback_value = 0.
        to_float_withfb = SaferFloat(with_fallback=fallback_value)
        to_float = SaferFloat(with_fallback=None)

        self.assertRaises(ValueError, to_float, "no_number")
        self.assertEqual(to_float_withfb("no_number"), fallback_value, "Failed to fallback an exception")
        self.assertEqual(to_float("1.0"), 1.0, "Failed to cast to 1.0")

    def test_safer_bool(self):
        fallback_value = False
        to_bool_withfb = SaferBool(with_fallback=fallback_value)
        to_bool = SaferBool(with_fallback=None)

        self.assertRaises(ValueError, to_bool, "no_number")
        self.assertEqual(to_bool_withfb("no_number"), fallback_value, "Failed to fallback an exception")
        self.assertEqual(to_bool("y"), True, "Failed to cast y to True")
