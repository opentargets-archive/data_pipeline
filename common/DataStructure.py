import json


__author__ = 'andreap'



class JSONSerializable():
    def to_json(self):
        return json.dumps(self.__dict__)

    def load_json(self, data):
        self.__dict__.update(**json.loads(data))