from scipy.spatial import cKDTree
import sys
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import util
from util import evaluate


class NemoSolver:
    def __init__(self, df, capacity_col, weight_col, step_size=0.5, merge_factor=0.5, max_levels=20):
        self.required_attributes = ['x', 'y', 'latency', 'type', weight_col, capacity_col, 'cluster']

        self.df_nemo = df[self.required_attributes].copy()
        self.weight_col = weight_col
        self.step_size = step_size
        self.merge_factor = merge_factor

        self.max_index = len(df.index)

        # levels
        self.max_levels = max_levels
        self.df_nemo["level"] = 0
        self.opt_dict_levels = dict()

        # labels
        self.parent_col = "parent"
        self.av_col = "free_capacity"
        self.capacity_col = capacity_col
        self.unbalanced_col = "unbalanced"

        # resources
        self.df_nemo[self.av_col] = self.df_nemo[self.capacity_col]
        self.df_nemo[self.unbalanced_col] = self.df_nemo[self.weight_col]

        # assignments
        self.c_indices = self.df_nemo[self.df_nemo["type"] == "coordinator"].index.values
        self.assignment_thresh = 10  # int(self.df_nemo[self.weight_col].median()) + 1

        # clusters
        self.centroids = self.df_nemo[self.df_nemo['cluster'] >= 0].groupby('cluster')[['x', 'y']].mean()
        self.kd_tree_centroids = cKDTree(self.centroids[["x", "y"]].to_numpy())
        self.unique_clusters = list(self.centroids.index)
        self.num_clusters = len(self.unique_clusters)

        # knn search
        self.df_nemo["oindex"] = self.df_nemo.index
        self.knn_nodes = set(self.df_nemo.index)
        self.knn_nodes.remove(0)

        # cluster head id
        self.df_nemo[self.parent_col] = [[]] * self.df_nemo.shape[0]

        self.children_dict = {}
        self.placement = False

    def nemo_full(self):
        if self.placement:
            raise RuntimeError("NEMO already terminated. Use re-optimize or re-instantiate NEMOSolver")

        current_cluster_heads = dict()
        sink = 0

        # pre-processing
        for cluster in self.unique_clusters:
            upstream_nodes = self.df_nemo[self.df_nemo["cluster"] == cluster].index
            current_cluster_heads[cluster] = list(upstream_nodes)

        self.opt_dict_levels, resource_limit = self.nemo_placement(current_cluster_heads, sink)
        placement_df = self.expand_df(self.capacity_col)
        self.placement = True

        return placement_df, self.opt_dict_levels, resource_limit

    def nemo_placement(self, upstream_nodes_dict, downstream_node, level=0):
        if self.df_nemo.at[0, self.av_col] == sys.maxsize:
            av = sys.maxsize
        else:
            av = self.df_nemo.at[downstream_node, self.av_col]
        resource_limit = False
        opt_dict_levels = {}
        prev_length_ch = 0

        while True:
            opt_dict = {}
            all_chs = list(set(item for sublist in upstream_nodes_dict.values() for item in sublist))

            if level > 0:
                self.remove_parents(all_chs)

            self.df_nemo.loc[all_chs, "level"] = level
            # has to be weight_col to avoid any cycles in the graph
            load = self.df_nemo.loc[all_chs, self.unbalanced_col].sum()
            print("Level", level, "CH number: ", len(all_chs), "Load:", load)

            if av >= load or level == self.max_levels or resource_limit or len(all_chs) == prev_length_ch:
                # set parent of cluster heads to downstream node
                for node_idx in all_chs:
                    n_load = self.df_nemo.loc[node_idx, self.unbalanced_col]
                    self.add_parent(node_idx, downstream_node, n_load)

                remaining_cap = self.df_nemo.loc[downstream_node, self.av_col]
                if remaining_cap < 0:
                    print("final assignment ------------------Load reached", downstream_node, remaining_cap)
                    resource_limit = True
                break
            else:
                new_cluster_heads = {}
                prev_length_ch = len(all_chs)

                if level >= 1:
                    current_num_clusters = len(upstream_nodes_dict.keys())
                    upstream_nodes_dict = self.merge_clusters_kmeans(all_chs, current_num_clusters, self.merge_factor)

                print("--------Balancing load for", len(upstream_nodes_dict.keys()), "clusters to", downstream_node)
                cl_cnt = 1
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

    def get_closest_clusters(self, coords, k=1):
        clusters = self.kd_tree_centroids.query(coords, k=k)[1]
        return clusters

    def update_coordinates(self, new_coords_df):
        self.df_nemo["x"] = new_coords_df["x"]
        self.df_nemo["y"] = new_coords_df["y"]
        self.df_nemo["latency"] = new_coords_df["latency"]
        self.df_nemo["cluster"] = new_coords_df["cluster"]

        # clusters
        self.centroids = self.df_nemo[self.df_nemo['cluster'] >= 0].groupby('cluster')[['x', 'y']].mean()
        self.kd_tree_centroids = cKDTree(self.centroids[["x", "y"]].to_numpy())
        self.unique_clusters = list(self.centroids.index)
        self.num_clusters = len(self.unique_clusters)

    def remove_nodes(self, node_ids, threshold=0, step_size=0, merge_factor=0):
        resource_limit = False
        if not self.placement:
            raise RuntimeError("NEMO not run. Use nemo_full first.")

        set_nodes = set(node_ids)
        if not set_nodes <= {*list(self.df_nemo.index)}:
            raise ValueError("Nodes missing in index with IDs", node_ids)

        if step_size > 0:
            self.step_size = step_size
        if merge_factor > 0:
            self.merge_factor = merge_factor
        self.assignment_thresh = threshold

        level = self.df_nemo.loc[node_ids, "level"].min()

        # identify all nodes that are leaf nodes and remove them
        ch_nodes = []
        affected_children = set()

        for node_id in node_ids:
            if node_id in self.children_dict:
                ch_nodes.append(node_id)
                children = self.children_dict[node_id].copy()
                # remove node from the "parent" column of its children
                for child_idx in children:
                    self.remove_parent(child_idx, node_id)
                    affected_children.add(child_idx)

            # release resources of parents
            self.remove_parents([node_id])
            # remove node from data structures
            self.df_nemo = self.df_nemo.drop(node_id)
            if node_id in self.knn_nodes:
                self.knn_nodes.remove(node_id)

        affected_children = list(affected_children.difference(set_nodes))
        affected_children = self.df_nemo.loc[affected_children].groupby("cluster").groups

        # affected_clusters = self.df_nemo.loc[affected_children, "cluster"].unique()
        # affected_children = self.df_nemo[self.df_nemo["cluster"].isin(affected_clusters)]
        # self.remove_parents(list(affected_children.index))
        # affected_children = affected_children.groupby("cluster").groups

        # handle cluster heads now
        if len(affected_children) > 0:
            # node is a cluster head, re-allocate the remaining load
            print(f"Node with ID {ch_nodes} are cluster head. Re-optimizing children with level {level}:",
                  affected_children)
            self.opt_dict_levels, resource_limit = self.nemo_placement(affected_children, 0, level=max(level - 1, 0))

        df_placement = self.expand_df(self.capacity_col)
        return df_placement, self.opt_dict_levels, resource_limit, level

    def add_nodes_to_df(self, nodes):
        # Columns to check for in the DataFrame
        required_keys = ['x', 'y', 'weight', 'capacity']
        ids = []

        for node in nodes:
            if not all(key in node for key in required_keys):
                raise ValueError('Attributes are missing for node', node)

            node_row = {'x': node['x'], 'y': node['y'], self.weight_col: node['weight'],
                        self.capacity_col: node['capacity']}
            node_coords = np.array([node['x'], node['y']])

            # calc following attributes: ['latency', 'type', 'free_slots', 'oindex', 'cluster', 'level', 'parent']
            node_row['latency'] = np.linalg.norm(node_coords - self.get_coords([0]))
            node_row['type'] = 'worker'
            node_row[self.av_col] = node_row[self.capacity_col]
            node_row[self.unbalanced_col] = node_row[self.weight_col]
            clusters = self.get_closest_clusters(node_coords, k=self.num_clusters)
            node_row['cluster'] = clusters[0]
            node_row['level'] = 0
            node_row['parent'] = []

            # add row to df_nemo and assign index
            self.max_index += 1
            if node["oindex"]:
                node_idx = node["oindex"]
            else:
                node_idx = self.max_index

            node_row['oindex'] = node_idx
            self.df_nemo.loc[node_idx] = node_row
            ids.append(node_idx)
        return ids

    def add_nodes(self, nodes, full_opt=False, threshold=0, step_size=0, merge_factor=0, k_cluster_opt=1):
        self.assignment_thresh = threshold
        if step_size > 0:
            self.step_size = step_size
        if merge_factor > 0:
            self.merge_factor = merge_factor

        node_ids = self.add_nodes_to_df(nodes)

        if not full_opt:
            reopt_dict = self.df_nemo.loc[node_ids].groupby("cluster").groups
        else:
            clusters = self.df_nemo.loc[node_ids, "cluster"].unique()

            if k_cluster_opt <= 1:
                knn_clusters = clusters
            else:
                knn_clusters = set()
                for cl in clusters:
                    n_clusters = self.get_closest_clusters(self.centroids.loc[cl].to_numpy(), k=k_cluster_opt)
                    knn_clusters.update(n_clusters)

            affected_nodes = self.df_nemo[self.df_nemo['cluster'].isin(knn_clusters)]
            self.remove_parents(list(affected_nodes.index))
            reopt_dict = affected_nodes.groupby("cluster").groups

        print("re-optimizing clusters", reopt_dict.keys())
        self.opt_dict_levels, resource_limit = self.nemo_placement(reopt_dict, 0, level=0)
        placement_df = self.expand_df(self.capacity_col)
        return placement_df, self.opt_dict_levels, resource_limit, 0

    def add_node(self, node, threshold=0, step_size=0, merge_factor=0):
        self.assignment_thresh = threshold
        if step_size > 0:
            self.step_size = step_size
        if merge_factor > 0:
            self.merge_factor = merge_factor

        node_idx = self.add_nodes_to_df([node])[0]
        node_row = self.df_nemo.loc[node_idx]
        cluster = node_row["cluster"]
        print("Adding node with index", node_idx)

        # calc the parents
        cluster_nodes = self.df_nemo[self.df_nemo['cluster'] == cluster].index
        parents = util.get_nested_parents(cluster_nodes, self.df_nemo, parent_col=self.parent_col)

        av_df = self.df_nemo.loc[parents, [self.av_col, 'latency']]
        av_df = av_df[av_df[self.av_col] > 0].sort_values(by='latency', ascending=True)
        av_resources = av_df[self.av_col].sum()

        if av_resources >= node_row[self.weight_col]:
            # cluster parents have enough resources, simply add node as child
            parents, limit = self.assign_resources([node_idx], list(av_df.index))
            placement_df = self.expand_df(self.capacity_col)
            return node_idx, placement_df, limit
        else:
            clusters_to_search = [cluster]
            print("balancing required for clusters", clusters_to_search)
            reopt_clusters = self.df_nemo[self.df_nemo['cluster'].isin(clusters_to_search)]
            # parents dont have enough resources, re-optimize the k-nearest clusters
            self.remove_parents(list(reopt_clusters.index))
            upstream_nodes_dict = reopt_clusters.groupby('cluster').groups
            print("reoptimizing clusters", upstream_nodes_dict.keys())

            self.opt_dict_levels, resource_limit = self.nemo_placement(upstream_nodes_dict, 0, level=1)

            placement_df = self.expand_df(self.capacity_col)
            return node_idx, placement_df, resource_limit

    def merge_clusters_kmeans(self, cluster_heads, current_num_clusters, merge_factor):
        idxs = cluster_heads
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
        parents = set()
        failed = False

        while from_idxs:
            if len(to_idxs) == 0:
                print("Assigning resources failed. Remaining nodes can not be re-balanced", from_idxs)
                parents.update(from_idxs)
                failed = True
                break

            parent_idx = to_idxs[0]
            av_resources = self.df_nemo.at[parent_idx, self.av_col]

            if av_resources <= self.assignment_thresh:
                idx = to_idxs.pop(0)
                if idx in self.knn_nodes:
                    self.knn_nodes.remove(idx)
                continue

            child_idx = from_idxs[0]
            unbalanced_load = self.df_nemo.at[child_idx, self.unbalanced_col]

            if av_resources >= unbalanced_load:
                # update values of the cluster head
                from_idxs.pop(0)
            else:
                unbalanced_load = av_resources

            self.add_parent(child_idx, parent_idx, unbalanced_load)

            unbalanced_load = self.df_nemo.at[child_idx, self.unbalanced_col]
            if unbalanced_load < 0:
                print("------------------Load reached", child_idx, unbalanced_load)

            parents.add(parent_idx)

        return parents, failed

    def remove_parents(self, node_ids):
        for node_id in node_ids:
            parents = self.df_nemo.at[node_id, self.parent_col]
            unbalanced_load = self.df_nemo.at[node_id, self.unbalanced_col]

            # update parent capacities
            for parent_idx, used_weight in parents:
                unbalanced_load += used_weight
                self.remove_parent_update_capacities(node_id, parent_idx, used_weight)

            # empty parents
            self.df_nemo.at[node_id, self.parent_col] = []
            # assign new unbalanced load
            self.df_nemo.at[node_id, self.unbalanced_col] = unbalanced_load

            unbalanced_load = self.df_nemo.at[node_id, self.unbalanced_col]
            assert unbalanced_load > 0, f"Negative load reached for node {node_id}: {unbalanced_load}"

        return self.df_nemo

    def remove_parent(self, node_id, parent_id):
        parents = self.df_nemo.at[node_id, self.parent_col]
        new_parents = []
        unbalanced_load = self.df_nemo.at[node_id, self.unbalanced_col]

        # update parent capacities
        for parent_idx, used_weight in parents:
            if parent_id == parent_idx:
                unbalanced_load += used_weight
                self.remove_parent_update_capacities(node_id, parent_id, used_weight)
            else:
                new_parents.append((parent_idx, used_weight))

        # empty parents
        self.df_nemo.at[node_id, self.parent_col] = new_parents
        # assign new unbalanced load
        self.df_nemo.at[node_id, self.unbalanced_col] = unbalanced_load

        unbalanced_load = self.df_nemo.at[node_id, self.unbalanced_col]
        assert unbalanced_load > 0, f"Negative load reached for node {node_id}: {unbalanced_load}"

        return unbalanced_load, self.df_nemo

    def remove_parent_update_capacities(self, node_id, parent_id, used_weight):
        # print(node_id, parent_id, self.df_nemo.at[node_id, self.parent_col], self.children_dict[parent_id])

        self.df_nemo.at[parent_id, self.av_col] += used_weight
        if parent_id != 0:
            self.knn_nodes.add(parent_id)

        self.children_dict[parent_id].remove(node_id)
        if len(self.children_dict[parent_id]) == 0:
            self.df_nemo.at[parent_id, "level"] = 0
            del self.children_dict[parent_id]

    def add_parent(self, child_idx, parent_idx, mapped_resources):
        if mapped_resources == 0:
            print("Error adding 0 to", child_idx, "->", parent_idx)

        if parent_idx not in self.children_dict:
            self.children_dict[parent_idx] = set()
        self.children_dict[parent_idx].add(child_idx)

        # update parent resources
        self.df_nemo.at[parent_idx, self.av_col] = self.df_nemo.at[parent_idx, self.av_col] - mapped_resources

        # update balance at added node
        self.df_nemo.at[child_idx, self.unbalanced_col] -= mapped_resources
        # update routes of the added node
        self.df_nemo.at[child_idx, self.parent_col] = self.df_nemo.at[child_idx, self.parent_col] + [
            (parent_idx, mapped_resources)]

    def expand_df(self, slot_col):
        rows = []
        max_idx = self.df_nemo['level'].idxmax()
        if max_idx > 0:
            self.df_nemo.at[0, "level"] = self.df_nemo.at[max_idx, "level"] + 1

        # Iterate over rows in the DataFrame
        for idx, row in self.df_nemo.iterrows():
            if not row["parent"]:
                # handle the sinks
                new_row = {'oindex': row['oindex'], 'x': row['x'], 'y': row['y'], 'type': row['type'],
                           'cluster': row['cluster'],
                           'total_weight': pd.NA, 'unbalanced_weight': pd.NA, 'used_weight': pd.NA,
                           'total_capacity': row[slot_col],
                           'free_capacity': row[self.av_col], 'level': row['level'], 'parent': pd.NA}
                rows.append(new_row)
            else:
                for item in row['parent']:
                    children = self.children_dict[item[0]]
                    if idx not in children:
                        raise RuntimeError(f"{idx} not in children of {item[0]}")

                    # Create a new row with 'type', 'tuple_element_1', 'tuple_element_2', and 'prev_index' columns
                    new_row = {'oindex': row['oindex'], 'x': row['x'], 'y': row['y'], 'type': row['type'],
                               'cluster': row['cluster'],
                               'total_weight': row[self.weight_col], 'unbalanced_weight': row[self.unbalanced_col],
                               'used_weight': item[1], 'total_capacity': row[slot_col],
                               'free_capacity': row[self.av_col], 'level': row['level'], 'parent': item[0]}
                    rows.append(new_row)

        # Create a new DataFrame from the list of rows
        out = pd.DataFrame(rows)
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
