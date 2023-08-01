import numpy as np
from scipy.spatial import cKDTree
import heapq


def euclidean_distance(p1, p2):
    return np.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)


def prim_mst(coordinates):
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
