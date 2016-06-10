import json
from UserDict import UserDict
from json import JSONEncoder

__author__ = 'andreap'

class PipelineEncoder(JSONEncoder):
    def default(self, o):
        try:
            return o.to_json()
        except AttributeError:
            pass
        return o.__dict__

class JSONSerializable():
    def to_json(self):
        return json.dumps(self,
                          default=lambda o: o.__dict__,
                          sort_keys=True,
                          # indent=4,
                          cls=PipelineEncoder)

    def load_json(self, data):
        if isinstance(data, str) or isinstance(data, unicode):
            self.__dict__.update(**json.loads(data))
        elif isinstance(data, dict):#already parsed json obj
            self.__dict__.update(**data)
        else:
            raise AttributeError("cannot load object from %s type"%str(type(data)))

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