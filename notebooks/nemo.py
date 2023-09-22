import sys

import numpy as np
import math

from resource_reassignment import get_cluster_heads
from util import evaluate

from scipy.spatial import cKDTree


class NemoSolver:
    def __init__(self, df_coords, coords, W, L):
        self.df_nemo = df_coords.copy()
        self.coords = coords
        self.W = W
        self.L = L

        self.device_number = len(df_coords.index)
        self.latency_hist = np.zeros(self.device_number)
        self.received_packets_hist = np.zeros(self.device_number)
        self.c_coords = self.df_nemo[self.df_nemo["type"] == "coordinator"][["x", "y"]].values[0]
        self.c_indices = self.df_nemo[self.df_nemo["type"] == "coordinator"].index.values

        self.area = W * L
        self.k = math.sqrt(self.area / self.device_number)
        self.t = W / 10

        self.unique_clusters = self.df_nemo[self.df_nemo['cluster'] >= 0]['cluster'].unique().tolist()
        self.num_clusters = len(self.unique_clusters)

        # cluster head id
        self.df_nemo["parent"] = 0
        self.df_nemo["route"] = [[0]] * len(self.df_nemo)

    def calc_route(self, i):
        parent_idx = self.df_nemo.at[i, "parent"]

        while True:
            self.df_nemo.at[i, "route"].append(parent_idx)
            if parent_idx != 0:
                parent_idx = self.df_nemo.at[parent_idx, "parent"]
            else:
                break

    def nemo(self, slot_col, iterations=100):
        ch_idxs_dict = {}
        ch_routes_dict = {}
        self.df_nemo["free_slots"] = self.df_nemo[slot_col]

        # max iterations
        area = self.W * self.L
        k = math.sqrt(area / self.device_number)
        t = self.W / 10

        # attractive force
        def f_a(d, k):
            return d * d / k

        def calc_F(v, u, k):
            D = v - u
            delta = np.linalg.norm(D)
            if delta != 0:
                d = f_a(delta, k) / delta
                DD = D * d
                return DD
            return 0

        # we use spring relaxation based on nemo
        # for each agg operator which is equal to num_clusters
        lns = 0
        total_assigned = 0
        total_iterated = 0

        # Build the k-d tree for the knn search
        kdtree = cKDTree(self.coords)

        for cluster in self.unique_clusters:
            # initial coordinates of the agg operator
            S = self.c_coords
            n_idx = self.df_nemo.index[self.df_nemo['cluster'] == cluster].tolist()
            lns += len(n_idx)

            for i in range(iterations):
                # for each parent node (source) in the group
                F = 0
                for node in n_idx:
                    F += +calc_F(S, self.coords[node], k)

                # same for the child (sink)
                F += +calc_F(S, self.c_coords, k)
                disp = np.linalg.norm(F)
                d = min(disp, t) / disp
                F = F * d
                S = S - F

            cluster_idx = kdtree.query([S[0], S[1]], k=1)[1]
            self.df_nemo, ch_dict, ch_routes, remaining = get_cluster_heads(cluster_idx, self.df_nemo)
            ch_idxs_dict[cluster] = list(ch_dict.keys())
            ch_routes_dict[cluster] = ch_routes
            # print("Cluster", cluster, "CH", ch_dict.keys(), "Remaining", remaining)
            assert (len(remaining) == 0)

        return self.df_nemo, ch_idxs_dict, ch_routes_dict


def evaluate_nemo(prim_df, coords, W, L, slot_cols, iterations=100):
    eval_dict = {}
    df_dict = {}
    ch_idx_dict = {}
    ch_routes_dict = {}

    for slot_col in slot_cols:
        print("Starting nemo for ", slot_col)
        nemo = NemoSolver(prim_df, coords, W, L)
        df_nemo, ch_idx_dict[slot_col], ch_routes_dict[slot_col] = nemo.nemo(slot_col, iterations=iterations)
        df_stats = evaluate(df_nemo, coords)
        eval_dict[slot_col] = df_stats.copy()
        df_dict[slot_col] = df_nemo[["cluster", slot_col, "parent", "route", "weight"]]

    return df_dict, eval_dict, ch_idx_dict, ch_routes_dict
