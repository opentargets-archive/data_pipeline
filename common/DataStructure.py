import json


__author__ = 'andreap'



class JSONSerializable():
    def to_json(self):
        return json.dumps(self.__dict__)

    def load_json(self, data):
        if isinstance(data, str) or isinstance(data, unicode):
            self.__dict__.update(**json.loads(data))
        elif isinstance(data, dict):#already parsed json obj
            self.__dict__.update(**data)
        else:
            raise AttributeError("datatype %s is not supported"%str(type(data)))

class NetworkNode():

        def __init__(self,
                     id='',
                     label='',
                     synonyms=[],
                     description='',
                     children=[],
                     parents=[],
                     anchestors=[],
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
            self.ancestors = anchestors
            self.parents = parents
            self.is_root = is_root


class OntologyNode(NetworkNode):

        def __init__(self,
                     uri='',
                     uri_code='',
                     ontology_name='',
                     ):
            super(NetworkNode, self).__init__()

            self.uri = uri
            self.uri_code = uri_code
            self.ontology_name = ontology_name