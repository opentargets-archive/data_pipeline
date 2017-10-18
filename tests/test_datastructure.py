import json
import unittest

import mrtarget.common.DataStructure as dt
from toolz import curry


class DataStructureTests(unittest.TestCase):
    def test_denormdict(self):
        d = {'a': 1, 'b': 2}

        self.assertTrue(dt.denormDict(d) == ({'a': 0.0, 'b': 0.0},
                                             {1: 0.0, 2: 0.0}),
                        'Failed to denormalise the dict')

    def test_sparsefloatdict(self):
        fd = dt.SparseFloatDict()

        self.assertTrue(fd['missing_number'] == 0.,
                        'Failed to return a default 0.')

    def test_treenode(self):
        tn = dt.TreeNode('1', 'label1', ['syn1', 'syn2'], 'description',
                         children=[dt.TreeNode('2')], parents=[dt.TreeNode('4')],
                         ancestors=[dt.TreeNode('5')], descendant=[dt.TreeNode('8')],
                         path=['path1'], is_root=True)

        self.assertTrue(tn != None, 'Failed? it should not')

    def test_jsonserializable(self):
        js = dt.JSONSerializable()

        js.load_json('{"name":"value"}')
        self.assertEqual(js.name, 'value', "Failed to deserialise from json string")

        js.load_json({'name2': 'value2'})
        self.assertEqual(js.name2, 'value2', "Failed to deserialise from json string")

        self.assertRaises(AttributeError, curry(js.load_json, 4))

