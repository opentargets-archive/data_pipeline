'''imported from cttv.model'''
import hashlib
import json
from collections import OrderedDict
from difflib import Differ
import logging


class DatatStructureFlattener:
    '''Class to flatten nested Python data structures into ordered dictionaries
    and to compute hexadigests of them when serialised as JSON. Used to compute
    hexadigests for JSON represented as Python data structures so that
    sub-structure order and white space are irrelvant.

    '''
    def __init__(self, data_structure):
        self.data_structure= data_structure

    def flatten(self, structure, key="", path="", flattened=None):
        '''
        Given any Python data structure nested to an arbitrary level, flatten it into an
        ordered dictionary. This method can be improved and simplified.
        Returns a Python dictionary where the levels of nesting are represented by
        successive arrows ("->").
        '''
        if flattened is None:
            flattened = {}
        if type(structure) not in(dict, list):
            flattened[((path + "->") if path else "") + key] = structure
        elif isinstance(structure, list):
            structure.sort()
            for i, item in enumerate(structure):
                self.flatten(item, "%d" % i, path + "->" + key, flattened)
        else:
            for new_key, value in structure.items():
                self.flatten(value, new_key, path + "->" + key, flattened)
        return flattened
    def get_ordered_dict(self):
        '''
        Return an ordered dictionary by processing the standard Python dictionary
        produced by method "flatten()".
        '''
        unordered_dict = self.flatten(self.data_structure)
        ordered_dict = OrderedDict()
        sorted_keys = sorted(unordered_dict.keys())
        for key in sorted_keys:
            key_cleaned = key.strip().replace('->->', '')
            ordered_dict[key_cleaned] = unordered_dict[key]
        return ordered_dict
    def get_hexdigest(self):
        '''
        Return the hexadigest value for a JSON-serialised version of the
        ordered dictionary returned by method "get_ordered_dict()".
        '''
        ordered_dict = self.get_ordered_dict()
        return hashlib.md5(json.dumps(ordered_dict)).hexdigest()
