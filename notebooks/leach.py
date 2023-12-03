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

    def leachClustering(self, df_rtt=None, rtt_clustering=False, rtt_eval=False):
        # cluster in which the node belongs to
        self.df_leach["cluster"] = -1
        # node role
        self.df_leach["role"] = 0
        # cluster head id
        self.df_leach["parent"] = 0
        # distance
        self.df_leach["distance"] = 0

        # threshold value
        if df_rtt is not None:
            sample_idxs = list(df_rtt[df_rtt.loc[0] > 0].index)
        else:
            sample_idxs = range(1, self.device_number)

        ch_indices = random.sample(sample_idxs, min(self.clusters, len(sample_idxs)))
        ch_coords = np.array(self.df_leach.loc[ch_indices, ['x', 'y']].apply(tuple, axis=1))

        # Build the k-d tree for the centroids
        kdtree = cKDTree(np.vstack(ch_coords))
        # Query the k-d tree to find the closest centroid for each node
        closest_centroids_indices = kdtree.query(self.coords, k=1)[1]

        # assign the cluster heads
        cluster_head_cnt = 0
        for i in ch_indices:
            if rtt_clustering:
                distance = df_rtt.loc[0, i]
            else:
                distance = int(np.linalg.norm(ch_coords[cluster_head_cnt] - self.c_coords))

            self.df_leach.loc[i, "role"] = 1
            self.df_leach.loc[i, "parent"] = -1
            self.df_leach.loc[i, "distance"] = distance
            self.df_leach.loc[i, "cluster"] = cluster_head_cnt

            if rtt_eval:
                self.latency_hist[i] = df_rtt.loc[0, i]
            else:
                self.latency_hist[i] = distance

            cluster_head_cnt = cluster_head_cnt + 1

        for i in range(1, self.device_number):
            if self.df_leach.loc[i, "cluster"] == -1:
                if rtt_clustering:
                    parent_idx = df_rtt.loc[i, ch_indices].idxmin()
                    ch_label = ch_indices.index(parent_idx)
                    dist = df_rtt.loc[i, parent_idx]
                else:
                    ch_label = closest_centroids_indices[i]
                    parent_idx = ch_indices[ch_label]
                    dist = int(np.linalg.norm(self.coords[i] - ch_coords[ch_label]))

                self.df_leach.loc[i, "cluster"] = ch_label
                self.df_leach.loc[i, "distance"] = dist

                self.df_leach.loc[i, "parent"] = parent_idx
                self.received_packets_hist[parent_idx] += 1
                if rtt_eval:
                    self.latency_hist[i] = df_rtt.loc[i, parent_idx] + df_rtt.loc[0, parent_idx]
                else:
                    dist = int(np.linalg.norm(self.coords[i] - ch_coords[ch_label]))
                    self.latency_hist[i] = dist + np.linalg.norm(ch_coords[ch_label] - self.c_coords)
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
