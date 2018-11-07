'''imported from cttv.model'''
import hashlib
import json
from collections import OrderedDict
from difflib import Differ
import logging


logger = logging.getLogger(__name__)


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

    diffLines = [line.strip() for line in result if line[0] != ' ']
    logger.warning(msg+"\n%s", "\t".join(diffLines))

    return True

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
