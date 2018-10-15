import logging
from distutils.util import strtobool

_l = logging.getLogger(__name__)

class SaferCast(object):
    """
    SaferCast represents a customised cast function with and optional way to bypass any exception
    by a fallback value.

    This base class wraps the generalisation and common functions
    """
    @staticmethod
    def _catch_with_fallback(func, value, fallback):
        """
        It executes func(value) wrapped by try catch finally if fallback is not None

        :param func: function to execute with at least one formal parameter
        :param value: the value to be passed to func(value)
        :param fallback: if is not None then the fallback will be used instead raising exceptions
        :return: func(value) if it raises an exception and fallback is None the exception is thrown
            if fallback is not None then fallback will be returned if func(value) raises exceptions
        """
        if fallback is not None:
            v = fallback
            try:
                v = func(value)
            except:
                _l.warning("fallback '%s' was used as the str value to cast was '%s' but caused an exception",
                           v, value)
                pass
            finally:
                return v
        else:
            return func(value)

    def __init__(self, func, with_fallback=None):
        """
        Create an object representing a higher order funcion calling func bypassing exceptions if
        with_fallback is not None and returning the later instead the raised one.

        :param func: the function to be used later
        :param with_fallback: if is not None func(v) will be wrapped and if exception will
            return with_fallback
        """
        self._logger = logging.getLogger(__name__)

        if callable(func):
            self._logger.debug("save callable object and set lambda for a later use")
            self._fallback = lambda v: SaferCast._catch_with_fallback(func, v, with_fallback)

        else:
            msg = "constructor parameter must be a callable, function or method (lambda included)"
            self._logger.debug(msg + ", but you passed %s", str(type(func)))
            raise TypeError(msg)

    def __call__(self, value):
        """
        call func(value) with_fallback if it is not None
        :param value:
        :return: func(value) or with_fallback if an exception was thrown
        """
        return self._fallback(value)


class SaferBool(SaferCast):
    """
    safer way to cast to a boolean from a string. the string provided is not case-sensitive

    The domain to mean True is defined as: 'true', '1', 't', 'y', 'yes' and to mean False is
    defined as: 'false', '0', 'f', 'n', 'no'. If with_fallback is None and string is not in either
    True domain or False domain a ValueError will be raised
    """
    @staticmethod
    def _str_to_boolean(s):
        bool(strtobool(s.lower()))

    def __init__(self, with_fallback):
        super(SaferBool, self).__init__(SaferBool._str_to_boolean, with_fallback=with_fallback)


class SaferInt(SaferCast):
    def __init__(self, with_fallback):
        super(SaferInt, self).__init__(int, with_fallback=with_fallback)


class SaferFloat(SaferCast):
    def __init__(self, with_fallback):
        super(SaferFloat, self).__init__(float, with_fallback=with_fallback)
