class FactNode:
    def __init__(self, fact: dict):
        self.stmt_node = fact['stmt_node']
        self.triples = fact['triples']
        self.external_variable = fact['external_variable']
        self.number_of_temporal_external_variables = fact['number_of_temporal_external_variables']
        self.neighbours = list()
        self.entities = self.get_entities()
        self.time_vars = self.get_time_vars()

    def get_entities(self):
        entities = set()
        for triple in self.triples:
            if triple[0].startswith('wd:'):
                entities.add(triple[0])
            if triple[2].startswith('wd:'):
                entities.add(triple[2])
        return entities
    
    def get_time_vars(self):
        time_vars = set()
        for var in self.external_variable:
            if var['temporal_value']:
                time_vars.add(var['variable'])
        return time_vars

class FactGraph:
    def __init__(self):
        self.nodes = list()
        self.var2node = dict()

    def add_nodes(self, nodes: list[FactNode]):
        self.nodes.extend(nodes)
        for node in nodes:
            for var in node.external_variable:
                var_name = var['variable']
                if var_name not in self.var2node.keys():
                    self.var2node[var_name] = [node]
                else:
                    for connected_node in self.var2node[var_name]:
                        connected_node.neighbours.append(node)
                        node.neighbours.append(connected_node)
                    self.var2node[var_name].append(node)
    
    def get_time_vars(self):
        time_vars = set()
        for node in self.nodes:
            time_vars = time_vars.union(node.get_time_vars())
        return time_vars

class FactGraphTraverser:
    def __init__(self, graph: FactGraph):
        self.graph = graph
        self.time_vars = dict()
    
    def start_traverse(self):
        time_vars = self.graph.get_time_vars()
        for var in time_vars:
            nodes = self.graph.var2node[var]
            for node in nodes:
                self.bfs(var, node, set(), 0)

    def bfs(self, time_var: str, node: FactNode, visited: set[FactNode], depth: int):
        visited.add(node)
        if len(node.entities) > 0:
            if time_var not in self.time_vars.keys():
                self.time_vars[time_var] = depth
            elif depth < self.time_vars[time_var]:
                self.time_vars[time_var] = depth
            return
        for neighbour in node.neighbours:
            if neighbour not in visited:
                self.bfs(time_var, neighbour, visited, depth + 1)


if __name__ == '__main__':
    facts = [
                {
                    "stmt_node": "?x4",
                    "triples": [
                        [
                            "wd:Q2798",
                            "p:P286",
                            "?x4"
                        ],
                        [
                            "?x4",
                            "ps:P286",
                            "?x0"
                        ],
                        [
                            "?x4",
                            "pq:P580",
                            "?x2"
                        ]
                    ],
                    "external_variable": [
                        {
                            "variable": "?x0",
                            "temporal_value": False
                        },
                        {
                            "variable": "?x2",
                            "temporal_property": "pq:P580",
                            "temporal_value": True,
                            "temporal_type": "time_point"
                        }
                    ],
                    "number_of_temporal_external_variables": 1
                },
                {
                    "stmt_node": None,
                    "triples": [
                        [
                            "?x0",
                            "wdt:P31",
                            "wd:Q5"
                        ]
                    ],
                    "external_variable": [
                        {
                            "variable": "?x0",
                            "temporal_value": False
                        }
                    ],
                    "number_of_temporal_external_variables": 0
                },
                {
                    "stmt_node": None,
                    "triples": [
                        [
                            "?x0",
                            "wdt:P569",
                            "?x3"
                        ]
                    ],
                    "external_variable": [
                        {
                            "variable": "?x0",
                            "temporal_value": False
                        },
                        {
                            "variable": "?x3",
                            "temporal_property": "wdt:P569",
                            "temporal_value": True,
                            "temporal_type": "time_point"
                        }
                    ],
                    "number_of_temporal_external_variables": 1
                }
            ]
    nodes = [FactNode(fact) for fact in facts]
    graph = FactGraph()
    graph.add_nodes(nodes)
    traverser = FactGraphTraverser(graph)
    traverser.start_traverse()
    traverse_result = traverser.time_vars
    for var in traverse_result.keys():
        print(f"Time var: {var}, depth: {traverse_result[var]}")
