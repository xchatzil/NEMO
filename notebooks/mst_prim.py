import numpy as np
from scipy.spatial import cKDTree
import heapq
import sys


def euclidean_distance(p1, p2):
    return np.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)


def prim_mst(coordinates, df_rtt=None):
    if df_rtt is not None:
        g = Graph(df_rtt.shape[0])
        g.graph = df_rtt.values
        return g.primMST()

    num_nodes = len(coordinates)
    visited = np.zeros(num_nodes, dtype=bool)
    parents = [-1] * num_nodes

    # Start with the first node as the root
    visited[0] = True

    # Build the k-d tree for efficient nearest neighbor search
    kdtree = cKDTree(coordinates)

    # Priority queue to keep track of the closest edge to the MST
    priority_queue = []
    for neighbor_idx in kdtree.query(coordinates[0], k=num_nodes)[1]:
        heapq.heappush(priority_queue, (euclidean_distance(coordinates[0], coordinates[neighbor_idx]), neighbor_idx, 0))

    while priority_queue:
        # Get the closest edge from the priority queue
        distance, node_idx, parent_idx = heapq.heappop(priority_queue)

        if not visited[node_idx]:
            # Add the edge to the MST and update the parent
            parents[node_idx] = parent_idx
            visited[node_idx] = True

            # Update the priority queue with the new closest edges
            for neighbor_idx in kdtree.query(coordinates[node_idx], k=num_nodes)[1]:
                if not visited[neighbor_idx]:
                    heapq.heappush(priority_queue, (
                        euclidean_distance(coordinates[node_idx], coordinates[neighbor_idx]), neighbor_idx, node_idx))

    return parents


def createRoutes(mst):
    routes = {}
    for i in range(1, len(mst)):
        route = []
        j = i
        # route from one node
        while True:
            node = mst[j]
            route.append(node)
            j = mst[j]
            if j == 0:
                routes[i] = route.copy()
                break
    return routes


class Graph():
    def __init__(self, vertices):
        self.V = vertices
        self.graph = [[0 for column in range(vertices)]
                      for row in range(vertices)]

    # A utility function to print
    # the constructed MST stored in parent[]
    def printMST(self, parent):
        print("Edge \tWeight")
        for i in range(1, self.V):
            print(parent[i], "-", i, "\t", self.graph[i][parent[i]])

    # A utility function to find the vertex with
    # minimum distance value, from the set of vertices
    # not yet included in shortest path tree
    def minKey(self, key, mstSet):

        # Initialize min value
        min = sys.maxsize

        for v in range(self.V):
            if key[v] < min and mstSet[v] == False:
                min = key[v]
                min_index = v

        return min_index

    # Function to construct and print MST for a graph
    # represented using adjacency matrix representation
    def primMST(self):

        # Key values used to pick minimum weight edge in cut
        key = [sys.maxsize] * self.V
        parent = [None] * self.V  # Array to store constructed MST
        # Make key 0 so that this vertex is picked as first vertex
        key[0] = 0
        mstSet = [False] * self.V

        parent[0] = -1  # First node is always the root of

        for cout in range(self.V):

            # Pick the minimum distance vertex from
            # the set of vertices not yet processed.
            # u is always equal to src in first iteration
            u = self.minKey(key, mstSet)

            # Put the minimum distance vertex in
            # the shortest path tree
            mstSet[u] = True

            # Update dist value of the adjacent vertices
            # of the picked vertex only if the current
            # distance is greater than new distance and
            # the vertex in not in the shortest path tree
            for v in range(self.V):

                # graph[u][v] is non zero only for adjacent vertices of m
                # mstSet[v] is false for vertices not yet included in MST
                # Update the key only if graph[u][v] is smaller than key[v]
                if self.graph[u][v] > 0 and mstSet[v] == False \
                        and key[v] > self.graph[u][v]:
                    key[v] = self.graph[u][v]
                    parent[v] = u

        return parent
