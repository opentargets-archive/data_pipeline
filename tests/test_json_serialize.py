from builtins import range
import json
import unittest

from mrtarget.common.DataStructure import JSONSerializable


class SerializeStub(JSONSerializable):

    def __init__(self,
                 id,
                 **kwargs):
        self.id = id
        self.__dict__.update(**kwargs)

class RedisWorkerTestCase(unittest.TestCase):
    def test_set_serializer(self):
        set_test = SerializeStub('set-test',
                                 set=set(range(10)))
        encoded_set_test = json.loads(set_test.to_json())
        self.assertTrue(isinstance(encoded_set_test['set'],list))
