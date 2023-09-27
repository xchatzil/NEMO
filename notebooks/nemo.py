from scipy.spatial import cKDTree
import sys
import numpy as np
import math

from resource_reassignment import get_cluster_heads
from util import evaluate


class NemoSolver:
    def __init__(self, df_coords, coords, centroids, W, L, weighting="spring", threshold=0.2, iterations=100,
                 weight_col="weight"):
        self.df_nemo = df_coords.copy()
        self.coords = coords
        self.W = W
        self.L = L
        self.weighting = weighting
        self.req_col = weight_col
        self.threshold = threshold
        self.iterations = iterations

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
        self.centroids = centroids

        self.full_kdtree = cKDTree(self.df_nemo[['x', 'y']])

        # cluster head id
        self.df_nemo["parent"] = [[]] * len(self.df_nemo)

    def get_logical_opt(self, s1, s2, k=2):
        def f_a(d, k):
            # attractive force
            return d * d / k

        def calc_F(v, u, k):
            D = v - u
            delta = np.linalg.norm(D)
            if delta != 0:
                d = f_a(delta, k) / delta
                DD = D * d
                return DD
            return 0

        t = self.W / 10
        F = 0
        for i in range(self.iterations):
            F += +calc_F(s1, s2, k)
            disp = np.linalg.norm(F)
            d = min(disp, t) / disp
            F = F * d
            s2 = s2 - F
        return s2

    def nemo(self, slot_col):
        ch_idxs_dict = {}
        av_col_label = "free_slots"
        self.df_nemo[av_col_label] = self.df_nemo[slot_col]
        level = -2

        # we use spring relaxation for each agg operator which is equal to num_clusters
        for cluster in self.unique_clusters:
            # Filter the DataFrame to get the group of elements with the same label
            group_df = self.df_nemo[self.df_nemo["cluster"] == cluster].copy()

            # Check if the group has enough resources
            total_required = group_df[self.req_col].sum()
            if self.df_nemo.at[0, av_col_label] == sys.maxsize:
                total_available = sys.maxsize
            else:
                total_available = group_df[av_col_label].sum()
            assert total_available >= total_required, str(total_available) + "<" + str(total_required)

            # calculate the logical optimum in continuous space
            if self.weighting == "centroid":
                opt = self.centroids[cluster]
            else:
                if cluster in self.centroids:
                    s1 = self.centroids[cluster]
                else:
                    s1 = group_df[["x", "y"]].mean()
                opt = self.get_logical_opt(s1, self.c_coords)

            # an optional threshold to avoid mapping to very small nodes
            res_threshold = int(self.threshold * group_df[av_col_label].median())

            # create index for efficient knn search
            kdtree = cKDTree(group_df[['x', 'y']])
            idx_order = kdtree.query([opt[0], opt[1]], k=group_df.shape[0])[1]
            sorted_df = group_df.iloc[idx_order]
            elements = list(zip(sorted_df.index, sorted_df[av_col_label], sorted_df[self.req_col]))

            # perform re-assignment to get the new cluster heads
            self.df_nemo, ch_dict = get_cluster_heads(elements, self.df_nemo, res_threshold, av_col_label, "parent")
            ch_idxs_dict[cluster] = list(ch_dict.keys())
            self.df_nemo.loc[ch_idxs_dict[cluster], 'cluster'] = level

        while True:
            group_df = self.df_nemo[self.df_nemo["cluster"] == level].copy()
            total_required = group_df[self.req_col].sum()

            if total_required <= self.df_nemo.at[0, av_col_label]:
                self.df_nemo.loc[ch_idxs_dict.keys(), "parent"] = self.df_nemo.loc[ch_idxs_dict.keys(), "parent"] + [0]
                return self.df_nemo, ch_idxs_dict
            else:
                ch_idxs_dict, level = self.add_level(group_df, ch_idxs_dict, level)

    def add_level(self, df, ch_idxs_dict, level):
        return level - 1


def calc_routes(df):
    return df


def evaluate_nemo(prim_df, coords, centroids, W, L, slot_cols, iterations=100, weighting="spring", weight_col="weight"):
    eval_dict = {}
    df_dict = {}
    ch_idx_dict = {}
    ch_routes_dict = {}

    for slot_col in slot_cols:
        print("Starting nemo for", slot_col, "with", weighting, "and", weight_col)
        nemo = NemoSolver(prim_df, coords, centroids, W, L, iterations=iterations, weighting=weighting,
                          weight_col=weight_col)
        df_dict[slot_col], ch_idx_dict[slot_col] = nemo.nemo(slot_col)
        # ch_routes_dict[slot_col] = calc_routes(df_nemo)
        # df_stats = evaluate(df_nemo, coords)
        # eval_dict[slot_col] = df_stats.copy()
        # df_dict[slot_col] = df_nemo[["cluster", slot_col, "parent", "route", weight_col]]

    return df_dict, ch_idx_dict
