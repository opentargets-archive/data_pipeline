from builtins import str
from builtins import object
import simplejson as json
try:
    from UserDict import UserDict
except ImportError:
    from collections import UserDict

from datetime import datetime, date



class PipelineEncoder(json.JSONEncoder):
    def default(self, o):
        try:
            return o.to_json()
        except AttributeError:
            pass
        return {key: o.__dict__[key] for key in o.__dict__ if not key.startswith('_')} # remove private properties


def json_serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat(' ')
    elif isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, set):
        return list(obj)
    else:
        try:
            return obj.__dict__
        except AttributeError:
            raise TypeError('Type not serializable')




class JSONSerializable(object):
    def to_json(self):
        return json.dumps(self,
                          default=json_serialize,
                          sort_keys=True,
                          # indent=4,
                          cls=PipelineEncoder)

    def load_json(self, data):
        if isinstance(data, str) or isinstance(data, str):
            self.__dict__.update(**json.loads(data))
        elif isinstance(data, dict):#already parsed json obj
            self.__dict__.update(**data)
        else:
            raise AttributeError("cannot load JSON object from %s type"%str(type(data)))


class TreeNode(object):

        def __init__(self,
                     id='',
                     label='',
                     synonyms=[],
                     description='',
                     children=[],
                     parents=[],
                     ancestors=[],
                     descendant=[],
                     path=[],
                     is_root=False,
                     ):
            self.id = id
            self.label = label
            self.synonyms = synonyms
            self.description = description

            self.path = path
            self.children = children
            self.has_children = bool(children)
            self.descendant = descendant
            self.ancestors = ancestors
            self.parents = parents
            self.is_root = is_root


class SparseFloatDict(UserDict):

    def __missing__(self, key):
        return 0.

