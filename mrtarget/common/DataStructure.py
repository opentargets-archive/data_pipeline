import simplejson as json
from UserDict import UserDict

from datetime import datetime, date

from mrtarget.Settings import Config
from addict import Dict



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
        self.stamp_data_release()
        return json.dumps(self,
                          default=json_serialize,
                          sort_keys=True,
                          # indent=4,
                          cls=PipelineEncoder)

    def load_json(self, data):
        if isinstance(data, str) or isinstance(data, unicode):
            self.__dict__.update(**json.loads(data))
        elif isinstance(data, dict):#already parsed json obj
            self.__dict__.update(**data)
        else:
            raise AttributeError("cannot load JSON object from %s type"%str(type(data)))

    def stamp_data_release(self):
        self.__dict__['data_release'] = Config.RELEASE_VERSION.split('-')[-1]


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


class RelationType(object):

    SHARED_DISEASE = 'shared-disease'
    SHARED_TARGET = 'shared-target'


def denormDict(adict, defval=(0.0, 0.0)):
    """Return 2 dicts in a pair all initialised with `defval`.
    As following this example as test

    >>> d = {"a": 1, "b": 2}
    >>> (d1, d2) = denormDict(d)
    >>> d1
    {'a': 0.0, 'b': 0.0}
    >>> d2
    {1: 0.0, 2: 0.0}

    """
    # more pythonic and practically same O(n)
    return ({k: defval[0] for k, _ in adict.iteritems()},
            {v: defval[1] for _, v in adict.iteritems()})
