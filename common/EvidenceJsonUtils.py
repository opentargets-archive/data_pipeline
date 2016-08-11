'''imported from cttv.model'''
import hashlib
import json
from collections import OrderedDict
from difflib import Differ

import sys


def assertJSONEqual(a, b, msg='Values are not equal', keys = []):
    if not(isinstance(a, str) or isinstance(a, unicode)):
        if keys and isinstance(a, dict):
            a=dict((k,v) for k,v in a.items() if k in keys)
        a=json.dumps(a, indent=4, sort_keys=True)
    if not(isinstance(b, str) or isinstance(b, unicode)):
        if keys and isinstance(a, dict):
            b=dict((k,v) for k,v in b.items() if k in keys)
        b=json.dumps(b, indent=4, sort_keys=True)
    d = Differ()
    result = list(
        d.compare(a.splitlines(1),
                  b.splitlines(1)))
    for line in result:
        if line[0] != ' ':
            sys.stderr.writelines(result)
            raise ValueError(msg)
    return True

class DatatStructureFlattener:
    '''
    Class to flatten nested Python data structures into ordered dictionaries and to
    compute hexadigests of them when serialised as JSON.
    Used to compute hexadigests for JSON represented as Python data structures so that sub-structure
    order and white space are irrelvant.
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
class CompareJsons:
    '''
    Compare two Python data structures and report any differences between them.
    Used to compare Python data structures created from JSON serializations. White space
    and element order are ignored.
    This class takes two Python data structures and uses them to create two instances
    of "DatatStructureFlattener". It uses this class's methods to flatten the data structures
    and generate ordered dictionaries for each of them that are then tested for key differences
    using set operations.
    '''
    def __init__(self, data_structure1, data_structure2):
        self.data_structure1 = data_structure1
        self.data_structure2 = data_structure2
        self.data_structure_flatten1 = DatatStructureFlattener(self.data_structure1)
        self.data_structure_flatten2 = DatatStructureFlattener(self.data_structure2)
        self.data_structure1_od = self.data_structure_flatten1.get_ordered_dict()
        self.data_structure2_od = self.data_structure_flatten2.get_ordered_dict()
    def do_data_structures_differ(self):
        '''
        Check if the hexadigests for the flattened ordered dictionary representation of the two
        data structures are the same. If this method returns True, skip other checks.
        '''
        return self.data_structure_flatten1.get_hexdigest() == self.data_structure_flatten2.get_hexdigest()
    def get_key_change_summary_list(self):
        '''
        Report all key differences between the two ordered dictionaries as a list.
        '''
        keys_set1 = set(self.data_structure1_od.keys())
        keys_set2 = set(self.data_structure2_od.keys())
        key_change_set = keys_set1 ^ keys_set2
        change_summary = []
        for element in key_change_set:
            try:
                self.data_structure1_od[element]
                change_summary.append('%s is missing in data structure 2.' % (element,))
            except KeyError:
                change_summary.append('%s is missing in data structure 1.' % (element,))
        return change_summary
    def get_value_change_summary_list(self):
        '''
        Report all value differences between the two ordered dictionaries as a list.
        '''
        change_summary = []
        for key in self.data_structure1_od.keys():
            if self.data_structure1_od[key] != self.data_structure2_od[key]:
                change_summary.append(key)
        return change_summary