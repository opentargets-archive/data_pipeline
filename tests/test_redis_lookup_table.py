from mrtarget.common.Redis import RedisLookupTable, RedisLookupTableJson, RedisLookupTablePickle
import unittest
from mrtarget.common.connection import RedisManager, new_redis_client


class PicklableObject(object):
    value = 'this is a test object that can be serialized with pickle'

    def __eq__(self, other):
        return self.value

class LookupTableTestCase(unittest.TestCase):

    def test_base_lookup(self):
        with RedisManager(False, "localhost", 35000):
            r_server = new_redis_client("localhost", 35000)
            table = RedisLookupTable(r_server=r_server)

            test = 'this is a string test that does not need to be serialized'
            key = 'test_key'
            table.set(key, test)
            self.assertEquals(table.get(key), test)


    def test_json_lookup(self):
        with RedisManager(False, "localhost", 35000):
            r_server = new_redis_client("localhost", 35000)
            table = RedisLookupTableJson(r_server=r_server)

            test = {'json test':'this is a test object that can be serialized to json'}
            key = 'test_key'
            table.set(key, test)
            self.assertEquals(table.get(key), test)

    def test_pickle_lookup(self):
        with RedisManager(False, "localhost", 35000):
            r_server = new_redis_client("localhost", 35000)
            table = RedisLookupTablePickle(r_server=r_server)

            test = PicklableObject()
            key = 'test_key'
            table.set(key, test)
            self.assertEquals(table.get(key), test)