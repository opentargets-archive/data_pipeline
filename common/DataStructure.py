import simplejson as json
from ConfigParser import NoSectionError
from UserDict import UserDict
from simplejson import JSONEncoder

from datetime import datetime, date

from settings import Config

__author__ = 'andreap'

class PipelineEncoder(JSONEncoder):
    def default(self, o):
        try:
            return o.to_json()
        except AttributeError:
            pass
        return o.__dict__

def json_serialize(obj):
    if isinstance(obj, datetime):
        return obj.isoformat(' ')
    elif isinstance(obj, date):
        return obj.isoformat()
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
        self.__dict__['data_release'] = Config.RELEASE_VERSION


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


class OntologyNode(TreeNode):

        def __init__(self,
                     uri='',
                     uri_code='',
                     ontology_name='',
                     **kwargs
                     ):
            super(OntologyNode, self).__init__(**kwargs)

            self.uri = uri
            self.uri_code = uri_code
            self.ontology_name = ontology_name

class SparseFloatDict(UserDict):

    def __missing__(self, key):
        return 0.


class RelationType(object):

    SHARED_DISEASE = 'shared-disease'
    SHARED_TARGET = 'shared-target'