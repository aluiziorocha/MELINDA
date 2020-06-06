# Python program for implementation of Ford Fulkerson algorithm 
# This code is contributed by Neelam Yadav    
# Copyright: https://www.geeksforgeeks.org/ford-fulkerson-algorithm-for-maximum-flow-problem/

# from collections import defaultdict
import numpy as np


# This class represents a directed graph using adjacency matrix representation
class Graph:

    def __init__(self, graph):
        self.graph = graph  # residual graph
        self.ROW = len(graph)
        # self.COL = len(gr[0])

    '''Returns true if there is a path from source 's' to sink 't' in 
    residual graph. Also fills parent[] to store the path '''

    def BFS(self, s, t, parent):

        # Mark all the vertices as not visited 
        visited = [False] * (self.ROW)

        # Create a queue for BFS 
        queue = []

        # Mark the source node as visited and enqueue it 
        queue.append(s)
        visited[s] = True

        # Standard BFS Loop
        while queue:

            # Dequeue a vertex from queue and print it
            u = queue.pop(0)

            # Get all adjacent vertices of the dequeued vertex u 
            # If a adjacent has not been visited, then mark it 
            # visited and enqueue it 
            for ind, val in enumerate(self.graph[u]):
                if visited[ind] == False and val > 0:
                    queue.append(ind)
                    visited[ind] = True
                    parent[ind] = u

                    # If we reached sink in BFS starting from source, then return
        # true, else false 
        return True if visited[t] else False

    # Returns tne maximum flow from s to t in the given graph 
    def FordFulkerson(self, source, sink):

        # This array is filled by BFS and to store path 
        parent = [-1] * self.ROW

        max_flow = 0  # There is no flow initially

        # Augment the flow while there is path from source to sink 
        while self.BFS(source, sink, parent):

            # Find minimum residual capacity of the edges along the 
            # path filled by BFS. Or we can say find the maximum flow 
            # through the path found. 
            path_flow = float("Inf")
            s = sink
            while s != source:
                path_flow = min(path_flow, self.graph[parent[s]][s])
                s = parent[s]

                # Add path flow to overall flow
            max_flow += path_flow

            # update residual capacities of the edges and reverse edges 
            # along the path 
            v = sink
            while v != source:
                u = parent[v]
                self.graph[u][v] -= path_flow
                self.graph[v][u] += path_flow
                v = parent[v]

        return self.graph, max_flow


def printgraph(graph):
    for s in range(len(graph)):
        for t in range(len(graph)):
            if graph[s][t] > 0:
                print("{} -> {}: {}".format(s, t, graph[s][t]))


# Create a graph given in the above diagram   
graph = np.array([[0, 12, 8, 10, 0, 0, 0],
                  [0, 0, 0, 0, 12, 12, 0],
                  [0, 0, 0, 0, 8, 8, 0],
                  [0, 0, 0, 0, 10, 10, 0],
                  [0, 0, 0, 0, 0, 0, 16],
                  [0, 0, 0, 0, 0, 0, 16],
                  [0, 0, 0, 0, 0, 0, 0]])

origraph = np.copy(graph)
g = Graph(graph)

(resgraph, max_flow) = g.FordFulkerson(source=0, sink=6)

print("The original graph is:")
printgraph(origraph)

print("The maximum possible flow is %d " % max_flow)

flowgraph = np.zeros((len(graph), len(graph)), dtype=np.int16)

for s in range(len(graph)):
    for t in range(len(graph)):
        if origraph[s][t] > 0:
            flowgraph[s][t] = resgraph[t][s]

print("The graph with maximum flow is:")
printgraph(flowgraph)
