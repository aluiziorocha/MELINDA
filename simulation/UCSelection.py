from yafs.selection import Selection
import networkx as nx
# from networkx.algorithms.flow import edmonds_karp
# import matplotlib.pyplot as plt


class MinimunPath(Selection):

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        """
        Computes the minimun path among the source elemento of the topology and the localizations of the module

        Return the path and the identifier of the module deployed in the last element of that path
        """
        node_src = topology_src
        DES_dst = alloc_module[app_name][message.dst]

        # print ("GET PATH")
        # print ("\tNode _ src (id_topology): %i" % node_src)
        # print ("\tRequest service: %s " % message.dst)
        # print ("\tProcess serving that service: %s " % DES_dst)

        bestPath = []
        bestDES = []
        min_path_size = float('inf')
        myDigraph = graph2digraph(sim.topology.G)

        for des in DES_dst:  ## In this case, there are only one deployment
            dst_node = alloc_DES[des]
            print ("\t Looking the path to id_node: %i" % dst_node)

            path = list(nx.shortest_path(myDigraph, source=node_src, target=dst_node, weight='BW'))
            actual_path_size = nx.shortest_path_length(myDigraph, source=node_src, target=dst_node, weight='BW')

            # Calculo do fluxo maximo no grafo desde 1 ate 6
            # flow_value, flow_dict = list(nx.maximum_flow(myDigraph, _s=0, _t=6, capacity="BW"))
            # print("MAXFLOW VALUE: ", flow_value)
            # print("MAXFLOW DICT: ", flow_dict)

            print ("\t\t candidate to best path: " + str(path) + " Size = " + str(actual_path_size))

            if actual_path_size < min_path_size:
                min_path_size = actual_path_size
                minPath = path
                bestDES = des

        bestPath = [minPath]
        bestDES = [des]
        print ("best path is: ", bestPath, " with weigth ", min_path_size)
        print "---------------------------------------------------------"
        return bestPath, bestDES


"""
        Converte um tipo Graph para Digraph mantendo os atributos dos nodes e edges
"""


def graph2digraph(graph):
    myDigraph = nx.DiGraph()
    myDigraph.add_nodes_from(graph.nodes(data=True))
    myDigraph.add_edges_from(graph.edges(data=True))

    return myDigraph


class MinPath_RoundRobin(Selection):

    def __init__(self):
        self.rr = {}  # for a each type of service, we have a mod-counter

    def get_path(self, sim, app_name, message, topology_src, alloc_DES, alloc_module, traffic, from_des):
        """
        Computes the minimun path among the source elemento of the topology and the localizations of the module

        Return the path and the identifier of the module deployed in the last element of that path
        """
        node_src = topology_src
        DES_dst = alloc_module[app_name][message.dst]  # returns an array with all DES process serving

        if message.dst not in self.rr.keys():
            self.rr[message.dst] = 0

        # print ("GET PATH")
        # print ("\tNode _ src (id_topology): %i" % node_src)
        # print ("\tRequest service: %s " % (message.dst))
        # print ("\tProcess serving that service: %s (pos ID: %i)" % (DES_dst, self.rr[message.dst]))

        bestPath = []
        bestDES = []

        for ix, des in enumerate(DES_dst):
            if message.name == "RawVideo" or message.name == "IdentifyObject":
                if self.rr[message.dst] == ix:
                    dst_node = alloc_DES[des]

                    path = list(nx.shortest_path(sim.topology.G, source=node_src, target=dst_node, weight='BW'))

                    bestPath = [path]
                    bestDES = [des]

                    self.rr[message.dst] = (self.rr[message.dst] + 1) % len(DES_dst)
                    break
            else:  # message.name == "M.B"

                dst_node = alloc_DES[des]

                path = list(nx.shortest_path(sim.topology.G, source=node_src, target=dst_node, weight='BW'))
                if message.broadcasting:
                    bestPath.append(path)
                    bestDES.append(des)
                else:
                    bestPath = [path]
                    bestDES = [des]

        return bestPath, bestDES