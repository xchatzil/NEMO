from scipy.spatial import cKDTree
import sys
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import util
from util import evaluate


class NemoSolver:
    def __init__(self, df, capacity_col, weight_col, step_size=0.5, merge_factor=0.5, max_levels=20):
        self.required_attributes = ['x', 'y', 'latency', 'type', 'weight', capacity_col, 'cluster']

        self.df_nemo = df[self.required_attributes].copy()
        self.weight_col = weight_col
        self.step_size = step_size
        self.merge_factor = merge_factor

        self.device_number = len(df.index)
        self.latency_hist = np.zeros(self.device_number)
        self.received_packets_hist = np.zeros(self.device_number)

        # levels
        self.max_levels = max_levels
        self.df_nemo["level"] = 0
        self.opt_dict_levels = dict()

        # labels
        self.parent_col = "parent"
        self.av_col = "free_capacity"
        self.capacity_col = capacity_col

        # resources
        self.df_nemo[self.av_col] = self.df_nemo[self.capacity_col]

        # assignments
        self.c_indices = self.df_nemo[self.df_nemo["type"] == "coordinator"].index.values

        # clusters
        self.centroids = self.df_nemo[self.df_nemo['cluster'] >= 0].groupby('cluster')[['x', 'y']].mean()
        self.kd_tree_centroids = cKDTree(self.centroids[["x", "y"]].to_numpy())
        self.unique_clusters = list(self.centroids.index)
        self.num_clusters = len(self.unique_clusters)

        # knn search
        self.df_nemo["oindex"] = self.df_nemo.index
        self.knn_nodes = set(range(1, len(self.df_nemo)))

        # cluster head id
        self.df_nemo[self.parent_col] = [[]] * self.df_nemo.shape[0]

        self.df_placement = None

    def nemo_full(self):
        if self.df_placement is not None:
            raise RuntimeError("NEMO already terminated. Use re-optimize or re-instantiate NEMOSolver")

        current_cluster_heads = dict()
        sink = 0

        # pre-processing
        for cluster in self.unique_clusters:
            upstream_nodes = self.df_nemo[self.df_nemo["cluster"] == cluster].index
            current_cluster_heads[cluster] = list(upstream_nodes)

        self.opt_dict_levels, resource_limit = self.nemo_placement(current_cluster_heads, sink)
        self.df_placement = self.expand_df(self.capacity_col)

        return self.df_placement, self.opt_dict_levels, resource_limit

    def nemo_placement(self, upstream_nodes_dict, downstream_node, level=0):
        if self.df_nemo.at[0, self.av_col] == sys.maxsize:
            av = sys.maxsize
        else:
            av = self.df_nemo.iloc[downstream_node][self.av_col]
        resource_limit = False
        opt_dict_levels = {}

        while True:
            opt_dict = {}
            all_chs = [item for sublist in upstream_nodes_dict.values() for item in sublist]
            self.df_nemo.loc[all_chs, "level"] = level
            load = self.df_nemo.loc[all_chs, self.weight_col].sum()
            print("Level", level, "CH number: ", len(all_chs))

            if av >= load or level == self.max_levels or resource_limit:
                # set parent of cluster heads to downstream node
                for cluster in upstream_nodes_dict.keys():
                    for child_idx in upstream_nodes_dict[cluster]:
                        self.create_mapping(child_idx, downstream_node, self.df_nemo.loc[child_idx, self.weight_col])
                break
            else:
                new_cluster_heads = {}
                if level >= 1:
                    current_num_clusters = len(upstream_nodes_dict.keys())
                    upstream_nodes_dict = self.merge_clusters_kmeans(all_chs, current_num_clusters, self.merge_factor)

                print("--------Balancing load for", len(upstream_nodes_dict.keys()), "clusters to", downstream_node)
                cl_cnt = 0
                for cluster in upstream_nodes_dict.keys():
                    if cl_cnt % 10 == 0:
                        print("Clusters processed:", cl_cnt)
                    cl_cnt += 1

                    upstream_nodes = list(upstream_nodes_dict[cluster])
                    # idx_min = self.df_nemo.loc[upstream_nodes, "latency"].idxmin()
                    # opt = self.df_nemo.loc[idx_min, ["x", "y"]].to_numpy()
                    if len(upstream_nodes) > 1:
                        new_cluster_heads[cluster], opt = self.balance_load(upstream_nodes, [downstream_node], opt=None)
                    else:
                        opt = self.get_coords(upstream_nodes[0])
                        new_cluster_heads[cluster], opt = upstream_nodes, opt

                    opt_dict[cluster] = opt
                    if opt is None:
                        resource_limit = True
                        break
                upstream_nodes_dict = new_cluster_heads
            level += 1
            opt_dict_levels[level] = opt_dict

        # returns result df, and dict with the local optima without replication
        return opt_dict_levels, resource_limit

    def get_closest_cluster(self, coords):
        idx_loc = self.kd_tree_centroids.query(coords, k=1)[1]
        cluster = self.unique_clusters[idx_loc]
        return cluster

    def remove_parents(self, node_ids):
        for node_id in node_ids:
            parents = self.df_nemo.iloc[node_id][self.parent_col]
            self.df_nemo.at[node_id, self.parent_col] = []

            # update parent capacities
            for parent_idx, used_weight in parents:
                self.df_nemo.at[parent_idx, self.av_col] += used_weight
                self.df_placement.loc[self.df_placement["oindex"] == parent_idx, self.av_col] = self.df_nemo.loc[
                    parent_idx, self.av_col]
                self.knn_nodes.add(parent_idx)

        # remove nodes from placement df
        if self.df_placement is not None:
            self.df_placement = self.df_placement[~self.df_placement["oindex"].isin(node_ids)]

        return self.df_nemo, self.df_placement

    def remove_node(self, node_id):
        resource_limit = False

        if self.df_placement is None:
            raise RuntimeError("NEMO not run. Use nemo_full first.")

        # get upstream nodes, downstream nodes, level and weights
        plcmnt_node = self.df_placement[self.df_placement["oindex"] == node_id]
        if plcmnt_node.empty:
            raise ValueError("Node does not exist with ID", node_id)
        level = plcmnt_node.iloc[0]["level"]
        upstream_nodes = self.df_placement[self.df_placement[self.parent_col] == node_id]["oindex"].to_numpy()

        # release resources of parents
        self.remove_parents([node_id])

        # remove node from the "parent" column of its children
        for i, child_idx in enumerate(upstream_nodes):
            new_parents = []
            parents = self.df_nemo.loc[child_idx, self.parent_col]
            for parent_id, used_weight in parents:
                if parent_id != node_id:
                    new_parents.append((parent_id, used_weight))
            self.df_nemo.at[child_idx, self.parent_col] = new_parents

        # remove node from data structures
        self.df_nemo = self.df_nemo.drop(node_id)
        if node_id in self.knn_nodes:
            self.knn_nodes.remove(node_id)

        if len(upstream_nodes) == 0:
            # node is leaf node, nothing more to be done
            return self.df_placement, dict(), resource_limit, level
        else:
            # node is a cluster head, re-allocate children
            self.remove_parents(upstream_nodes)
            upstream_nodes_dict = {0: upstream_nodes}
            self.opt_dict_levels, resource_limit = self.nemo_placement(upstream_nodes_dict, 0, level=level - 1)
            self.df_placement = self.expand_df(self.capacity_col)
            return self.df_placement, self.opt_dict_levels, resource_limit, level

    def add_node(self, node):
        # Columns to check for in the DataFrame
        required_keys = ['x', 'y', 'weight', 'capacity']

        if not all(key in node for key in required_keys):
            raise ValueError('Attributes are missing', required_keys)

        node_row = {'x': node['x'], 'y': node['y'], self.weight_col: node['weight'],
                    self.capacity_col: node['capacity']}
        node_coords = np.array([node['x'], node['y']])

        # calc following attributes: ['latency', 'type', 'free_slots', 'oindex', 'cluster', 'level', 'parent']
        print(node_coords)
        node_row['latency'] = np.linalg.norm(node_coords - self.get_coords([0]))
        node_row['type'] = 'worker'
        node_row[self.av_col] = node_row[self.capacity_col]
        self.device_number += 1
        cluster = self.get_closest_cluster(node_coords)
        node_row['cluster'] = cluster
        node_row['level'] = 0
        node_row['parent'] = []

        # add row to df_nemo and assign index
        node_idx = len(self.df_nemo)
        node_row['oindex'] = node_idx
        self.df_nemo.loc[node_idx] = node_row

        # calc the parent
        cluster_nodes = self.df_nemo[self.df_nemo['cluster'] == cluster].index
        parents = self.df_placement[self.df_placement['cluster'] == cluster][self.parent_col].unique()
        av_df = self.df_nemo.loc[parents, [self.av_col, 'latency']]
        av_df = av_df[av_df[self.av_col] > 0].sort_values(by='latency', ascending=True)
        av_resources = av_df[self.av_col].sum()

        if av_resources >= node_row[self.weight_col]:
            # cluster parents have enough resources, simply add node as child
            parents, limit = self.assign_resources([node_idx], list(av_df.index))
            self.df_placement = self.expand_df(self.capacity_col)
            return node_idx, self.df_placement, parents, limit
        else:
            # parents dont have enough resources, re-optimize the cluster
            upstream_nodes = list(cluster_nodes)
            self.remove_parents(upstream_nodes)
            upstream_nodes_dict = {node_row['cluster']: upstream_nodes}
            self.opt_dict_levels, resource_limit = self.nemo_placement(upstream_nodes_dict, 0, level=0)
            self.df_placement = self.expand_df(self.capacity_col)
            parents = self.df_placement[self.df_placement['cluster'] == node_row['cluster']]['parent'].unique()
            return node_idx, self.df_placement, parents, resource_limit

    def merge_clusters_kmeans(self, cluster_heads, current_num_clusters, merge_factor):
        idxs = list(set(cluster_heads))
        coords = self.get_coords(idxs)
        out = {}
        num_clusters = min(current_num_clusters, len(idxs))
        num_clusters = max(int(merge_factor * num_clusters), 1)

        cluster_alg = KMeans(n_clusters=num_clusters, n_init='auto', random_state=42).fit(coords)
        labels = cluster_alg.labels_

        for i, idx in enumerate(idxs):
            l = labels[i]
            if l in out:
                out[l].append(idx)
            else:
                out[l] = [idx]

        return out

    def balance_load(self, upstream_nodes, downstream_node, opt=None):
        intersection = list(set(upstream_nodes).intersection(downstream_node))
        if intersection:
            print("Intersection found in balance load", intersection)

        if opt is None:
            # calculate logical opt for the operator in the continuous space
            s1 = self.get_coords(upstream_nodes).mean(axis=0)
            s2 = self.get_coords(downstream_node).mean(axis=0)

            w = self.step_size
            # w = len(upstream_nodes) / len(downstream_node)
            opt = util.calc_opt(s1, s2, w=w)

        # preform knn search
        # print("Performing knn for", [opt[0], opt[1]], "with k=", k)
        # df_knn = self.df_nemo.iloc[list(self.knn_nodes)]
        # full_kdtree = cKDTree(df_knn[["x", "y"]])

        knn_idxs = list(self.knn_nodes)
        knn_coords = self.get_coords(knn_idxs)
        full_kdtree = cKDTree(knn_coords)

        idx_order = full_kdtree.query([opt[0], opt[1]], k=len(self.knn_nodes))[1]
        idx_order = np.take(knn_idxs, idx_order)

        req = self.df_nemo.loc[upstream_nodes, self.weight_col].sum()
        if self.df_nemo.at[0, self.av_col] == sys.maxsize:
            sum_avs = sys.maxsize
        else:
            sum_avs = self.df_nemo.loc[idx_order, self.av_col].sum()

        if req > sum_avs:
            print("Topology does not contain enough available resources " + str(req) + "/" + str(sum_avs))
            return upstream_nodes, None

        upstream_nodes = self.df_nemo.loc[upstream_nodes, "latency"].sort_values(ascending=False).index.tolist()
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
                self.knn_nodes.remove(parent_idx)
                continue

            if tmp:
                child_idx, required = tmp
                split = True
            else:
                child_idx = from_idxs[0]
                required = self.df_nemo.loc[child_idx, self.weight_col]
                split = False

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
            if not row["parent"]:
                # handle the sinks
                new_row = {'oindex': row['oindex'], 'x': row['x'], 'y': row['y'], 'type': row['type'],
                           'cluster': row['cluster'],
                           'total_weight': pd.NA, 'used_weight': pd.NA,
                           'total_capacity': row[slot_col],
                           'free_capacity': row[self.av_col], 'level': row['level'], 'parent': pd.NA}
                rows.append(new_row)
            else:
                for item in row['parent']:
                    # Create a new row with 'type', 'tuple_element_1', 'tuple_element_2', and 'prev_index' columns
                    new_row = {'oindex': row['oindex'], 'x': row['x'], 'y': row['y'], 'type': row['type'],
                               'cluster': row['cluster'],
                               'total_weight': row[self.weight_col], 'used_weight': item[1],
                               'total_capacity': row[slot_col],
                               'free_capacity': row[self.av_col], 'level': row['level'], 'parent': item[0]}
                    rows.append(new_row)

        # Create a new DataFrame from the list of rows
        out = pd.DataFrame(rows)
        out.loc[0, 'level'] = max_level + 1
        return out

    def get_coords(self, node_ids):
        return self.df_nemo.loc[node_ids, ["x", "y"]].to_numpy()


def calc_routes(df):
    return df


def evaluate_nemo(prim_df, capacity_cols, weight_col, max_levels=20, step_size=0.5, merge_factor=0.5, with_eval=True):
    df_dict = {}
    opt_dict = {}
    limits_dict = {}
    eval_matrix_slots = {}

    for capacity_col in capacity_cols:
        print("Starting nemo for: c=" + str(capacity_col) + ", w=" + str(weight_col) + ", l=" + str(max_levels)
              + ", step_size=" + str(step_size) + ", merge_factor=" + str(merge_factor))
        nemo = NemoSolver(prim_df, capacity_col, weight_col, max_levels=max_levels, step_size=step_size,
                          merge_factor=merge_factor)
        df_dict[capacity_col], opt_dict[capacity_col], limits_dict[capacity_col] = nemo.nemo_full()
        if with_eval:
            print("Evaluating for", capacity_col)
            eval_matrix_slots[capacity_col] = evaluate(df_dict[capacity_col])

    if with_eval:
        return eval_matrix_slots, df_dict, opt_dict, limits_dict
    else:
        return df_dict, opt_dict, limits_dict
