from scipy.spatial import cKDTree
import sys
import numpy as np
import math
import pandas as pd

import util
from resource_reassignment import get_cluster_heads
from util import evaluate


class NemoSolver:
    def __init__(self, df_coords, coords, centroids, W, L, weighting="spring", threshold=0.1, iterations=100,
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

        # labels
        self.parent_col = "parent"
        self.av_col = "free_slots"

        # assignments
        self.c_coords = self.df_nemo[self.df_nemo["type"] == "coordinator"][["x", "y"]].values[0]
        self.c_indices = self.df_nemo[self.df_nemo["type"] == "coordinator"].index.values

        self.area = W * L
        self.k = math.sqrt(self.area / self.device_number)
        self.t = W / 10

        self.unique_clusters = self.df_nemo[self.df_nemo['cluster'] >= 0]['cluster'].unique().tolist()
        self.num_clusters = len(self.unique_clusters)
        self.centroids = centroids

        self.df_nemo["oindex"] = self.df_nemo.index
        self.df_knn = self.df_nemo.iloc[1:]
        self.full_kdtree = cKDTree(self.df_knn[["x", "y"]])

        # cluster head id
        self.df_nemo[self.parent_col] = [[]] * len(self.coords)
        self.df_nemo.at[0, "parent"] = [(0, 0)]

    def nemo(self, slot_col):
        self.df_nemo[self.av_col] = self.df_nemo[slot_col]
        self.df_nemo["level"] = 0
        current_cluster_heads = {}
        level = 0
        downstream_node = 0

        for cluster in self.unique_clusters:
            upstream_nodes = self.df_nemo[self.df_nemo["cluster"] == cluster].index
            current_cluster_heads[cluster] = list(upstream_nodes)

        while True:
            if self.df_nemo.at[0, self.av_col] == sys.maxsize:
                av = sys.maxsize
            else:
                av = self.df_nemo.loc[downstream_node, self.av_col].sum()
            all_chs = [item for sublist in current_cluster_heads.values() for item in sublist]
            self.df_nemo.loc[all_chs, "level"] = level
            load = self.df_nemo.loc[all_chs, "weight"].sum()

            if av >= load:
                # set parent of cluster heads to downstream node
                for cluster in current_cluster_heads.keys():
                    self.assign_resources(list(current_cluster_heads[cluster]), [downstream_node])
                break
            else:
                new_cluster_heads = {}
                for cluster in current_cluster_heads.keys():
                    upstream_nodes = current_cluster_heads[cluster]
                    new_cluster_heads[cluster] = self.replicate_and_place(list(upstream_nodes), [downstream_node])
                current_cluster_heads = new_cluster_heads
            level += 1
        return self.expand_df(slot_col)

    def replicate_and_place(self, upstream_nodes, downstream_node):
        # calculate logical opt for the operator in the continuous space
        s1 = self.df_nemo.iloc[upstream_nodes][["x", "y"]].mean()
        s2 = self.df_nemo.iloc[downstream_node][["x", "y"]].mean()
        opt = util.calc_opt(s1, s2, w1=0.1)

        # calculate k for the knn search
        req = self.df_nemo.loc[upstream_nodes, self.req_col].sum()
        av_per_node = self.df_nemo[self.av_col].median()
        sum_avs = 0
        k = 1000
        idx_order = []
        while (sum_avs < req) and (k <= self.df_nemo.shape[0] - 1):
            k = min(k, self.df_nemo.shape[0] - 1)

            # preform knn search
            # print("Performing knn for", [opt[0], opt[1]], "with k=", k)
            idx_order = self.full_kdtree.query([opt[0], opt[1]], k=k)[1]
            idx_order = self.df_knn['oindex'].iloc[idx_order].tolist()
            if self.df_nemo.at[0, self.av_col] == sys.maxsize:
                sum_avs = sys.maxsize
            else:
                sum_avs = self.df_nemo.loc[idx_order, self.av_col].sum()
                k += int(req / av_per_node) + k

        assert req <= sum_avs, "Topology does not contain enough available resources"
        return self.assign_resources(upstream_nodes, list(idx_order))

    def assign_resources(self, from_idxs, to_idxs):
        # res_threshold = int(self.threshold * self.df_nemo.loc[to_idxs, self.av_col].median())
        res_threshold = 0
        tmp = None
        parents = set()

        while from_idxs:
            parent_idx = to_idxs[0]
            av_resources = self.df_nemo.loc[parent_idx, self.av_col]

            if av_resources <= res_threshold:
                to_idxs.pop(0)
                continue

            if tmp:
                child_idx, required = tmp
            else:
                child_idx = from_idxs[0]
                required = self.df_nemo.loc[child_idx, self.req_col]

            if av_resources >= required:
                # update values of the cluster head
                new_available = av_resources - required
                self.create_mapping(child_idx, parent_idx, required, new_available)
                from_idxs.pop(0)
                tmp = None
            else:
                # not enough resources, split mapping
                self.create_mapping(child_idx, parent_idx, av_resources, 0)
                remaining_required = required - av_resources
                tmp = (child_idx, remaining_required)

            parents.add(parent_idx)
        return parents

    def create_mapping(self, child_idx, parent_idx, mapped_resources, new_parent_resources):
        # update parent resources
        self.df_nemo.at[parent_idx, self.av_col] = new_parent_resources
        # update routes of the added node
        self.df_nemo.at[child_idx, self.parent_col] = self.df_nemo.at[child_idx, self.parent_col] + [
            (parent_idx, mapped_resources)]

    def expand_df(self, slot_col):
        rows = []
        max_level = self.df_nemo['level'].max()
        # Iterate over rows in the DataFrame
        for idx, row in self.df_nemo.iterrows():
            for item in row['parent']:
                # Create a new row with 'type', 'tuple_element_1', 'tuple_element_2', and 'prev_index' columns
                new_row = {'oindex': row['oindex'], 'x': row['x'], 'y': row['y'], 'type': row['type'],
                           'cluster': row['cluster'],
                           'total_weight': row['weight'], 'used_weight': item[1], 'total_capacity': row[slot_col],
                           'free_capacity': row[self.av_col], 'level': row['level'], 'parent': item[0]}
                rows.append(new_row)

        # Create a new DataFrame from the list of rows
        out = pd.DataFrame(rows)
        out.loc[0, 'level'] = max_level + 1
        return out


def calc_routes(df):
    return df


def evaluate_nemo(prim_df, coords, centroids, W, L, slot_cols, iterations=100, weighting="spring", weight_col="weight"):
    df_dict = {}

    for slot_col in slot_cols:
        print("Starting nemo for", slot_col, "with", weighting, "and", weight_col)
        nemo = NemoSolver(prim_df, coords, centroids, W, L, iterations=iterations, weighting=weighting,
                          weight_col=weight_col)
        df_dict[slot_col] = nemo.nemo(slot_col)
        # ch_routes_dict[slot_col] = calc_routes(df_nemo)
        # df_stats = evaluate(df_nemo, coords)
        # eval_dict[slot_col] = df_stats.copy()
        # df_dict[slot_col] = df_nemo[["cluster", slot_col, "parent", "route", weight_col]]

    return df_dict
