from mrtarget.common.DataStructure import TreeNode




class NetworkHandler():

    def __init__(self,
                 root= 'root',
                 ):

        self.root = root
        self.nodes = {}

    def is_root(self, node):
        return node.id == self.root

    def add_node(self, node):
        if self._node_can_be_added(node):
            self.nodes[node.id]=node

    def _node_has_valid_parents(self, node):
        for parent in node.parents:
            if parent not in self.nodes:
                raise AttributeError("Parent node %s is not available in the network"%parent)
        return True

    def _node_can_be_added(self, node):
        if not isinstance(node, TreeNode):
            raise AttributeError("Only instances of NetworkNode can be added to the network, not: %s",str(type(node)))
        if not self._node_has_valid_parents(node):
            return False
        if node.id in self.nodes:
            raise AttributeError("Node %s is already in the network"%node.id)
        return True
