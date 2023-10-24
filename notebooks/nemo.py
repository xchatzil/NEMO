from scipy.spatial import cKDTree
import sys
import numpy as np
import math
import pandas as pd

import util
from resource_reassignment import get_cluster_heads
from util import evaluate


class NemoSolver:
    def __init__(self, df_coords, centroids, weighting="spring", threshold=0.1, iterations=100,
                 max_levels=20, weight_col="weight"):
        self.df_nemo = df_coords.copy()
        self.coords = self.df_nemo[["x", "y"]].to_numpy()
        self.weighting = weighting
        self.req_col = weight_col
        self.threshold = threshold
        self.opt_iterations = iterations
        self.max_levels = max_levels

        self.device_number = len(df_coords.index)
        self.latency_hist = np.zeros(self.device_number)
        self.received_packets_hist = np.zeros(self.device_number)

        # labels
        self.parent_col = "parent"
        self.av_col = "free_slots"

        # assignments
        self.c_coords = self.df_nemo[self.df_nemo["type"] == "coordinator"][["x", "y"]].values[0]
        self.c_indices = self.df_nemo[self.df_nemo["type"] == "coordinator"].index.values

        self.unique_clusters = self.df_nemo[self.df_nemo['cluster'] >= 0]['cluster'].unique().tolist()
        self.num_clusters = len(self.unique_clusters)
        self.centroids = centroids

        self.df_nemo["oindex"] = self.df_nemo.index
        self.knn_nodes = set(range(1, len(self.df_nemo)))

        # cluster head id
        self.df_nemo[self.parent_col] = [[]] * len(self.coords)
        self.df_nemo.at[0, "parent"] = [(0, 0)]

    def nemo(self, slot_col):
        self.df_nemo[self.av_col] = self.df_nemo[slot_col]
        self.df_nemo["level"] = 0
        current_cluster_heads = dict()
        opt_dict_iter = dict()
        level = 0
        downstream_node = 0

        for cluster in self.unique_clusters:
            upstream_nodes = self.df_nemo[self.df_nemo["cluster"] == cluster].index
            current_cluster_heads[cluster] = list(upstream_nodes)

        if self.df_nemo.at[0, self.av_col] == sys.maxsize:
            av = sys.maxsize
        else:
            av = self.df_nemo.loc[downstream_node, self.av_col].sum()
        resource_limit = False

        while True:
            opt_dict = {}
            all_chs = [item for sublist in current_cluster_heads.values() for item in sublist]
            self.df_nemo.loc[all_chs, "level"] = level
            load = self.df_nemo.loc[all_chs, "weight"].sum()

            if av >= load or level == self.max_levels or resource_limit:
                # set parent of cluster heads to downstream node
                for cluster in current_cluster_heads.keys():
                    for child_idx in current_cluster_heads[cluster]:
                        self.create_mapping(child_idx, downstream_node, self.df_nemo.loc[child_idx, self.req_col])
                break
            else:
                new_cluster_heads = {}
                if level >= 1:
                    current_cluster_heads = self.merge_clusters(current_cluster_heads, all_chs)
                for cluster in current_cluster_heads.keys():
                    upstream_nodes = current_cluster_heads[cluster]
                    new_cluster_heads[cluster], opt = self.balance_load(list(upstream_nodes), [downstream_node])
                    opt_dict[cluster] = opt
                    if opt is None:
                        resource_limit = True
                        break
                current_cluster_heads = new_cluster_heads
            level += 1
            opt_dict_iter[level] = opt_dict

        # returns result df, and dict with the local optima without replication
        return self.expand_df(slot_col), opt_dict_iter, resource_limit

    def merge_clusters(self, cluster_head_dict, cluster_heads):
        cluster_dict = {}
        mapping_dict = {}

        if len(cluster_heads) < 2:
            return cluster_head_dict

        for key, value_set in cluster_head_dict.items():
            for element in value_set:
                cluster_dict[element] = key

        df_knn = self.df_nemo.iloc[list(cluster_heads)]
        full_kdtree = cKDTree(df_knn[["x", "y"]])
        out = cluster_head_dict.copy()

        for cluster, nodes in cluster_head_dict.items():
            if len(nodes) == 1 and len(out[cluster]) == 1:
                node = list(nodes)[0]
                opt = self.df_nemo.loc[node, ["x", "y"]].tolist()
                idx_order = full_kdtree.query([opt[0], opt[1]], k=2)[1]
                idx_order = df_knn['oindex'].iloc[idx_order].tolist()[1]
                new_cluster = cluster_dict[idx_order]

                if cluster in out and cluster != new_cluster:
                    del out[cluster]

                while new_cluster in mapping_dict and cluster != new_cluster:
                    new_cluster = mapping_dict[new_cluster]

                if cluster != new_cluster:
                    mapping_dict[cluster] = new_cluster
                    out[new_cluster].update(nodes)

        return out

    def balance_load(self, upstream_nodes, downstream_node):
        intersection = list(set(upstream_nodes).intersection(downstream_node))
        if intersection:
            print("Intersection found in balance load", intersection)

        # calculate logical opt for the operator in the continuous space
        s1 = self.df_nemo.iloc[upstream_nodes][["x", "y"]].mean()
        s2 = self.df_nemo.iloc[downstream_node][["x", "y"]].mean()
        opt = util.calc_opt(s1, s2, w1=0.1)

        # preform knn search
        # print("Performing knn for", [opt[0], opt[1]], "with k=", k)
        df_knn = self.df_nemo.iloc[list(self.knn_nodes)]
        full_kdtree = cKDTree(df_knn[["x", "y"]])
        idx_order = full_kdtree.query([opt[0], opt[1]], k=len(self.knn_nodes))[1]
        idx_order = df_knn['oindex'].iloc[idx_order].tolist()

        req = self.df_nemo.loc[upstream_nodes, self.req_col].sum()
        if self.df_nemo.at[0, self.av_col] == sys.maxsize:
            sum_avs = sys.maxsize
        else:
            sum_avs = self.df_nemo.loc[idx_order, self.av_col].sum()

        if req > sum_avs:
            print("Topology does not contain enough available resources " + str(req) + "/" + str(sum_avs))
            return upstream_nodes, None

        parents, failed = self.assign_resources(upstream_nodes, list(idx_order))
        if failed:
            return upstream_nodes, None
        else:
            return parents, opt

    def assign_resources(self, from_idxs, to_idxs):
        # res_threshold = int(self.threshold * self.df_nemo.loc[to_idxs, self.av_col].median())
        res_threshold = 0
        tmp = None
        parents = set()
        failed = False

        while from_idxs:
            if not to_idxs:
                print("Assigning resources failed. Remaining nodes can not be re-balanced", from_idxs)
                parents.update(from_idxs)
                failed = True
                break

            parent_idx = to_idxs[0]
            av_resources = self.df_nemo.loc[parent_idx, self.av_col]

            if av_resources <= res_threshold:
                idx = to_idxs.pop(0)
                self.knn_nodes.remove(idx)
                continue

            if tmp:
                child_idx, required = tmp
                split = True
            else:
                child_idx = from_idxs[0]
                required = self.df_nemo.loc[child_idx, self.req_col]
                split = False

            if child_idx == parent_idx:
                idx = to_idxs.pop(0)
                self.knn_nodes.remove(idx)

            if av_resources >= required:
                # update values of the cluster head
                self.create_mapping(child_idx, parent_idx, required, split)
                from_idxs.pop(0)
                tmp = None
            else:
                # not enough resources, split mapping
                self.create_mapping(child_idx, parent_idx, av_resources, split)
                remaining_required = required - av_resources
                tmp = (child_idx, remaining_required)

            parents.add(parent_idx)
        return parents, failed

    def create_mapping(self, child_idx, parent_idx, mapped_resources, split=False):
        parents = self.df_nemo.at[child_idx, self.parent_col]
        for p, weight in parents:
            if p == parent_idx:
                return

        # update parent resources
        self.df_nemo.at[parent_idx, self.av_col] = self.df_nemo.at[parent_idx, self.av_col] - mapped_resources
        # update routes of the added node
        if split:
            self.df_nemo.at[child_idx, self.parent_col] = self.df_nemo.at[child_idx, self.parent_col] + [
                (parent_idx, mapped_resources)]
        else:
            for p, weight in parents:
                self.df_nemo.at[p, self.av_col] = self.df_nemo.at[p, self.av_col] + weight
            self.df_nemo.at[child_idx, self.parent_col] = [(parent_idx, mapped_resources)]

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


def evaluate_nemo(prim_df, centroids, slot_cols, max_levels=20, iterations=100, weighting="spring",
                  weight_col="weight", show_eval=True):
    df_dict = {}
    opt_dict = {}
    limits_dict = {}

    eval_matrix_slots = {}

    for slot_col in slot_cols:
        print("Starting nemo for", slot_col, "with", weighting, "and", weight_col, "and level:", max_levels)
        nemo = NemoSolver(prim_df, centroids, iterations=iterations, weighting=weighting,
                          max_levels=max_levels, weight_col=weight_col)
        df_dict[slot_col], opt_dict[slot_col], limits_dict[slot_col] = nemo.nemo(slot_col)
        if show_eval:
            print("Evaluating for", slot_col)
            coords = prim_df[["x", "y"]].to_numpy()
            eval_matrix_slots[slot_col] = evaluate(df_dict[slot_col], coords)

    return eval_matrix_slots, df_dict, opt_dict, limits_dict
