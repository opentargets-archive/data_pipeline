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