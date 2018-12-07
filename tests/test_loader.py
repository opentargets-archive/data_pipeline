import unittest
from mrtarget.common.ElasticsearchLoader import Loader


class ElasticsearchLoaderTestCase(unittest.TestCase):

    def test_put(self):
        loader = Loader(dry_run=True)
        loader.put('dummy-index',
                   'dummy-doctype',
                   'id',
                   '{"hello":"test"}')
        self.assertEquals(len(loader.cache),1)
        loader.flush()
        self.assertEquals(len(loader.cache),0)
        loader.close()

    def test_many_put(self):
        loader = Loader(dry_run=True,
                        chunk_size=100)
        for i in range(150):
            loader.put('dummy-index',
                       'dummy-doctype',
                       'id',
                       '{"hello":"test"}')
        self.assertEquals(len(loader.cache),50)
        loader.close()
        self.assertEquals(len(loader.cache),0)



