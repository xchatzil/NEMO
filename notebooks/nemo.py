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

        self.cluster_freq = self.df_nemo[self.df_nemo["cluster"] >= 0]["cluster"].value_counts()
        self.unique_clusters = self.cluster_freq.keys()
        self.num_clusters = len(self.unique_clusters)

        # cluster head id
        self.df_nemo["parent"] = 0
        # distance
        self.df_nemo["route"] = self.df_nemo.apply(lambda x: [], axis=1)
        self.df_nemo["route"] = self.df_nemo["route"].astype("object")

    def calc_route(self, i):
        parent_idx = self.df_nemo.at[i, "parent"]

        while True:
            self.df_nemo.at[i, "route"].append(parent_idx)
            if parent_idx != 0:
                parent_idx = self.df_nemo.at[parent_idx, "parent"]
            else:
                break

    def nemo(self, hierarchical, slot_col, iterations=100, redistribute=True):
        agg_points_per_cluster = {}
        cluster_path_dict = {}
        if redistribute:
            self.df_nemo["free_slots"] = self.df_nemo[slot_col]
        else:
            self.df_nemo["free_slots"] = sys.maxsize

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

        for cluster in range(0, self.num_clusters):
            cluster_path_dict[cluster] = []
            # initial coordinates of the agg operator
            S = self.c_coords
            n_idx = self.df_nemo.index[self.df_nemo['cluster'] == cluster].tolist()
            lns += len(n_idx)

            for i in range(iterations):
                # for each parent node (source) in the group
                F = 0
                for node in n_idx:
                    if hierarchical:
                        F += (1 / len(n_idx)) * +calc_F(S, self.coords[node], k)
                    else:
                        F += +calc_F(S, self.coords[node], k)

                # same for the child (sink)
                F += +calc_F(S, self.c_coords, k)
                disp = np.linalg.norm(F)
                d = min(disp, t) / disp
                F = F * d
                S = S - F

            cluster_idx = kdtree.query([S[0], S[1]], k=1)[1]
            av_slots = self.df_nemo.iloc[cluster_idx]["free_slots"]

            cluster_idxs = [(cluster_idx, av_slots)]

            required_slots = len(n_idx)
            if av_slots < required_slots:
                cluster_idxs = get_cluster_heads(self.df_nemo, "free_slots", cluster_idx, av_slots, required_slots)

            assigned = 0
            agg_points_per_cluster[cluster] = [idx[0] for idx in cluster_idxs]

            res = sum(int(i[1]) for i in cluster_idxs)
            # print("Numbers in cluster ", cluster, "->", len(n_idx), "/", res)
            assert (len(n_idx) <= res)
            total_iterated += len(n_idx)
            for cluster_idx, slots in cluster_idxs:
                lbl_agg = self.df_nemo.iloc[cluster_idx]["cluster"]

                if lbl_agg != cluster and hierarchical:
                    cluster_path_dict[cluster].append((cluster, lbl_agg))
                else:
                    cluster_path_dict[cluster].append((cluster_idx, 0))

                # update the route
                used_slots = 0
                for i in n_idx[assigned:assigned + slots]:
                    if hierarchical:
                        self.df_nemo.at[i, "parent"] = cluster_idx
                        if i == cluster_idx and lbl_agg == cluster:
                            self.df_nemo.at[i, "parent"] = 0
                    else:
                        if (i not in agg_points_per_cluster[cluster]) and cluster_idx != 0:
                            self.df_nemo.at[i, "route"] = [cluster_idx, 0]
                            self.df_nemo.at[i, "parent"] = cluster_idx
                        else:
                            self.df_nemo.at[i, "route"] = [0]
                            self.df_nemo.at[i, "parent"] = 0
                    used_slots += 1
                assigned = assigned + used_slots
                self.df_nemo.at[cluster_idx, "free_slots"] = self.df_nemo.iloc[cluster_idx]["free_slots"] - used_slots
            total_assigned += assigned

        if hierarchical:
            for i in range(1, self.device_number):
                self.calc_route(i)
        # print(self.device_number - 1, " ", lns, " ", total_assigned, " ", total_iterated)
        assert (self.device_number - 1 == lns == total_assigned == total_iterated)
        return agg_points_per_cluster, cluster_path_dict, self.df_nemo


def evaluate_nemo(prim_df, coords, W, L, hierarchical, slot_cols, iterations=100):
    eval_matrix_slots = {}
    path_dict = {}
    agg_dict = {}
    df_dict = {}

    # evaluate first the base alg without reassignment
    slot_col = "base"
    print("Starting nemo for ", slot_col)
    nemo = NemoSolver(prim_df, coords, W, L)
    agg_dict[slot_col], path_dict[slot_col], df_nemo = nemo.nemo(hierarchical, slot_col, iterations,
                                                                 redistribute=False)
    df_stats = evaluate(df_nemo, coords)
    eval_matrix_slots[slot_col] = df_stats.copy()
    df_dict[slot_col] = df_nemo[["cluster", "parent", "route"]]

    for slot_col in slot_cols:
        print("Starting nemo for ", slot_col)
        nemo = NemoSolver(prim_df, coords, W, L)
        agg_dict[slot_col], path_dict[slot_col], df_nemo = nemo.nemo(hierarchical, slot_col, iterations)
        df_stats = evaluate(df_nemo, coords)
        eval_matrix_slots[slot_col] = df_stats.copy()
        df_dict[slot_col] = df_nemo[["cluster", slot_col, "parent", "route"]]
    return eval_matrix_slots, path_dict, agg_dict, df_dict


def re_evaluate_nemo(prim_df, coords, W, L, hierarchical, slot_cols, iterations=100):
    eval_matrix_slots = {}
    path_dict = {}
    agg_dict = {}
    df_dict = {}

    # evaluate first the base alg without reassignment
    slot_col = "base"
    print("Starting nemo for ", slot_col)
    nemo = NemoSolver(prim_df, coords, W, L)
    agg_dict[slot_col], path_dict[slot_col], df_nemo = nemo.nemo(hierarchical, slot_col, iterations,
                                                                 redistribute=False)
    df_stats = evaluate(df_nemo, coords)
    eval_matrix_slots[slot_col] = df_stats.copy()
    df_dict[slot_col] = df_nemo[["cluster", "parent", "route"]]

    for slot_col in slot_cols:
        print("Starting nemo for ", slot_col)
        nemo = NemoSolver(prim_df, coords, W, L)
        agg_dict[slot_col], path_dict[slot_col], df_nemo = nemo.nemo(hierarchical, slot_col, iterations)
        df_stats = evaluate(df_nemo, coords)
        eval_matrix_slots[slot_col] = df_stats.copy()
        df_dict[slot_col] = df_nemo[["cluster", slot_col, "parent", "route"]]
    return eval_matrix_slots, path_dict, agg_dict, df_dict, df_nemo
