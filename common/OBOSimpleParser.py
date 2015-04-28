__author__ = 'andreap'

class Node():

    def __init__(self, id, name, definition = ''):
        self.id = id
        self.name = name
        self.definition = definition

    def __str__(self):
        return "id:%s | name:%s | definition:%s"%(self.id,
                                                   self.name,
                                                   self.definition)


class OBOParser():

    def __init__(self, filename):
        self.filename = filename

    def parse(self):
        single_node = []
        store = False
        for line in file(self.filename):
            if not line.strip():
                store = False
                if single_node:
                    yield self._parse_single_node(single_node)
                single_node = []
            if line.startswith('[Term]'):
                store = True
            if store:
                single_node.append(line)

    def _parse_single_node(self, single_node):
        data = dict(id = '',
                    name = '',
                    definition = '')
        current_field = ''
        for line in single_node:
            if line.startswith('id'):
                current_field = 'id'
                line = line.split(': ')[1]
            elif line.startswith('name'):
                current_field = 'name'
                line = line.split(': ')[1]
            elif line.startswith('def'):
                current_field = 'definition'
                line = line.split(': ')[1]
            elif ': ' in line:
                current_field = None
            if current_field:
                data[current_field]+=line.replace('\n', '')


        return Node(data['id'], data['name'], data['definition'])