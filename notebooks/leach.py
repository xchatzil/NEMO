import numpy as np
import random

import pandas as pd
from scipy.spatial import cKDTree
import skfuzzy as fuzz


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
            rtt_eval = False
            rtt_clustering = False
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

    def leachSFClustering(self, capacity_col, df_rtt=None, rtt_clustering=False, rtt_eval=False):
        def dist_func(row, u, max_c):
            dist = np.linalg.norm(row[["x", "y"]] - centers[row["cluster"], :])
            dist += row["latency"]
            dist = (1 - u[row["cluster"], row.name]) * dist
            dist = dist * row[capacity_col] / max_c
            return dist

        max_c = self.df_leach[capacity_col].max()

        # perform clustering
        centers, u, u0, d, jm, p, fpc = fuzz.cluster.cmeans(self.df_leach[["x", "y"]].values.T, c=self.clusters, m=2,
                                                            error=0.005, maxiter=1000, init=None)
        self.df_leach["cluster"] = np.argmax(u, axis=0)
        self.df_leach['distance'] = self.df_leach.apply(lambda row: dist_func(row, u, max_c), axis=1)

        self.df_leach.loc[0, "cluster"] = -1
        self.df_leach.loc[0, "distance"] = 0

        # identify cluster head for each cluster
        ch_indices = self.df_leach[self.df_leach["cluster"] >= 0].groupby("cluster")["distance"].idxmin().to_numpy()
        self.df_leach['parent'] = self.df_leach.groupby('cluster')['distance'].transform('idxmin')
        self.df_leach.loc[ch_indices, "parent"] = 0
        self.df_leach.loc[0, "parent"] = np.nan

        # cols for evaluation
        self.df_leach["oindex"] = self.df_leach.index
        self.df_leach["level"] = 0
        self.df_leach.loc[ch_indices, "level"] = 1
        self.df_leach.loc[0, "level"] = 2
        self.df_leach["total_capacity"] = self.df_leach[capacity_col]

        cl_sizes = self.df_leach[self.df_leach["cluster"] >= 0].groupby("cluster").size() + 1
        self.df_leach["free_capacity"] = 0
        self.df_leach.loc[ch_indices, "free_capacity"] = cl_sizes.to_numpy()
        self.df_leach.loc[0, "free_capacity"] = cl_sizes.shape[0]
        self.df_leach["free_capacity"] = self.df_leach["total_capacity"] - self.df_leach["free_capacity"]

        return centers, u, self.df_leach, ch_indices
