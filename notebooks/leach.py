import numpy as np
import random
from scipy.spatial import cKDTree


class LeachSolver:
    def __init__(self, df_coords, num_clusters, coords, seed=None):
        self.df_leach = df_coords.copy()
        self.coords = coords.copy()
        self.device_number = len(df_coords.index)
        self.latency_hist = np.zeros(self.device_number)
        self.received_packets_hist = np.zeros(self.device_number)
        self.clusters = num_clusters
        self.c_coords = self.df_leach[self.df_leach["type"] == "coordinator"][["x", "y"]].values[0]
        self.c_indices = self.df_leach[self.df_leach["type"] == "coordinator"].index.values
        if seed:
            random.seed(seed)

    def leachClustering(self):
        # cluster in which the node belongs to
        self.df_leach["cluster"] = -1
        # node role
        self.df_leach["role"] = 0
        # cluster head id
        self.df_leach["parent"] = 0
        # distance
        self.df_leach["distance"] = 0
        self.df_leach["distance"] = self.df_leach["distance"].astype(float)

        # threshold value
        ch_indices = random.sample(range(1, self.device_number), self.clusters)
        ch_coords = np.array(self.df_leach.iloc[ch_indices][['x', 'y']].apply(tuple, axis=1))

        # Build the k-d tree for the centroids
        kdtree = cKDTree(np.vstack(ch_coords))
        # Query the k-d tree to find the closest centroid for each node
        closest_centroids_indices = kdtree.query(self.coords, k=1)[1]

        # assign the cluster heads
        cluster_head_cnt = 0
        for i in ch_indices:
            distance = np.linalg.norm(ch_coords[cluster_head_cnt] - self.c_coords)
            self.df_leach.loc[i, "role"] = 1
            self.df_leach.loc[i, "parent"] = -1
            self.df_leach.loc[i, "distance"] = distance
            self.df_leach.loc[i, "cluster"] = cluster_head_cnt
            self.latency_hist[i] = distance
            cluster_head_cnt = cluster_head_cnt + 1

        for i in range(1, self.device_number):
            if self.df_leach.loc[i, "cluster"] == -1:
                ch_label = closest_centroids_indices[i]
                self.df_leach.loc[i, "cluster"] = ch_label
                dist = np.linalg.norm(self.coords[i] - ch_coords[ch_label])
                self.df_leach.loc[i, "distance"] = dist

                parent_idx = ch_indices[ch_label]
                self.df_leach.loc[i, "parent"] = parent_idx
                self.latency_hist[i] = dist + np.linalg.norm(ch_coords[ch_label] - self.c_coords)
                self.received_packets_hist[parent_idx] += 1
            else:
                self.received_packets_hist[0] += 1

        # Grouping the Nodes into Clusters & calculating the distance between node and cluster head
        # for i in range(0, self.device_number):
        #    if self.df_leach.loc[i, "role"] == 0 and cluster_head_cnt > 0 and i not in self.c_indices:
        #        # min_distance = np.linalg.norm(coords[i]-c_coords)
        #        coords_i = self.df_leach[["x", "y"]].loc[i].values
        #        ch_index, dist = knn(coords_i, cluster_head_indexes, self.coords)
        #        self.df_leach.loc[i, "cluster"] = self.df_leach.loc[ch_index, "cluster"]
        #        self.df_leach.loc[i, "distance"] = dist
        #        self.df_leach.loc[i, "parent"] = ch_index

        #        self.latency_hist[i] = dist + np.linalg.norm(self.coords[ch_index] - self.c_coords)
        #        self.received_packets_hist[ch_index] += 1
        #    else:
        #        self.received_packets_hist[0] += 1

        labels = self.df_leach["cluster"]
        labels = labels.tolist()
        return labels, ch_indices, self.latency_hist, self.received_packets_hist
