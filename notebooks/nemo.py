from scipy.spatial import cKDTree
import sys
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

import util
from util import evaluate


class NemoSolver:
    def __init__(self, df_coords, capacity_col, weight_col, step_size=0.5, merge_factor=0.5, max_levels=20):
        self.df_nemo = df_coords.copy()
        self.coords = self.df_nemo[["x", "y"]].to_numpy()
        self.weight_col = weight_col
        self.step_size = step_size
        self.merge_factor = merge_factor

        self.device_number = len(df_coords.index)
        self.latency_hist = np.zeros(self.device_number)
        self.received_packets_hist = np.zeros(self.device_number)

        # levels
        self.max_levels = max_levels
        self.df_nemo["level"] = 0
        self.opt_dict_levels = dict()

        # labels
        self.parent_col = "parent"
        self.av_col = "free_slots"
        self.capacity_col = capacity_col

        # resources
        self.df_nemo[self.av_col] = self.df_nemo[self.capacity_col]

        # assignments
        self.c_coords = self.df_nemo[self.df_nemo["type"] == "coordinator"][["x", "y"]].values[0]
        self.c_indices = self.df_nemo[self.df_nemo["type"] == "coordinator"].index.values

        self.unique_clusters = self.df_nemo[self.df_nemo['cluster'] >= 0]['cluster'].unique().tolist()
        self.num_clusters = len(self.unique_clusters)

        self.df_nemo["oindex"] = self.df_nemo.index
        self.knn_nodes = set(range(1, len(self.df_nemo)))

        # cluster head id
        self.df_nemo[self.parent_col] = [[]] * len(self.coords)

        self.terminated = False

    def nemo_full(self):
        if self.terminated:
            raise RuntimeError("NEMO already terminated. Use re-optimize or re-instantiate NEMOSolver")

        current_cluster_heads = dict()
        sink = 0

        # pre-processing
        for cluster in self.unique_clusters:
            upstream_nodes = self.df_nemo[self.df_nemo["cluster"] == cluster].index
            current_cluster_heads[cluster] = list(upstream_nodes)

        self.opt_dict_levels, resource_limit = self.nemo_placement(current_cluster_heads, sink)
        self.terminated = True

        return self.expand_df(self.capacity_col), self.opt_dict_levels, resource_limit

    def nemo_placement(self, upstream_nodes_dict, downstream_node, level=0):
        if self.df_nemo.at[0, self.av_col] == sys.maxsize:
            av = sys.maxsize
        else:
            av = self.df_nemo.loc[downstream_node, self.av_col].sum()
        resource_limit = False
        opt_dict_levels = {}

        while True:
            opt_dict = {}
            all_chs = [item for sublist in upstream_nodes_dict.values() for item in sublist]
            self.df_nemo.loc[all_chs, "level"] = level
            load = self.df_nemo.loc[all_chs, self.weight_col].sum()

            if av >= load or level == self.max_levels or resource_limit:
                # set parent of cluster heads to downstream node
                for cluster in upstream_nodes_dict.keys():
                    for child_idx in upstream_nodes_dict[cluster]:
                        self.create_mapping(child_idx, downstream_node, self.df_nemo.loc[child_idx, self.weight_col])
                break
            else:
                new_cluster_heads = {}
                if level >= 1:
                    # print(current_cluster_heads)
                    current_num_clusters = len(upstream_nodes_dict.keys())
                    upstream_nodes_dict = self.merge_clusters_kmeans(all_chs, current_num_clusters, self.merge_factor)

                for cluster in upstream_nodes_dict.keys():
                    upstream_nodes = list(upstream_nodes_dict[cluster])
                    # idx_min = self.df_nemo.loc[upstream_nodes, "latency"].idxmin()
                    # opt = self.df_nemo.loc[idx_min, ["x", "y"]].to_numpy()
                    if len(upstream_nodes) > 1:
                        new_cluster_heads[cluster], opt = self.balance_load(upstream_nodes, [downstream_node], opt=None)
                    else:
                        opt = self.df_nemo.loc[upstream_nodes[0], ["x", "y"]].to_numpy()
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

    def nemo_reoptimize(self, old_node, upstream_nodes, downstream_nodes, opt=None):
        if not self.terminated:
            raise RuntimeError("NEMO not run. Use nemo_full first.")

        resource_limit = False
        tree_reopt_level = np.nan

        new_cluster_heads = set()
        if old_node in self.knn_nodes:
            self.knn_nodes.remove(old_node)

        for downstream_node in downstream_nodes:
            if downstream_node in self.knn_nodes:
                self.knn_nodes.remove(downstream_node)

            level = self.df_nemo.loc[downstream_node, "level"] - 1

            while True:
                av = self.df_nemo.loc[downstream_node, self.av_col].sum()
                load = self.df_nemo.loc[upstream_nodes, self.weight_col].sum()

                if av >= load or resource_limit:
                    for child_idx in upstream_nodes:
                        self.create_mapping(child_idx, downstream_node, self.df_nemo.loc[child_idx, self.weight_col])
                        new_cluster_heads.add(downstream_node)
                    break
                else:
                    new_parents, opt = self.balance_load(upstream_nodes, [downstream_node], opt=opt)

                    if set(new_parents) == set(upstream_nodes):
                        tree_reopt = True
                        # convergence was during the previous level
                        level = level - 1
                        if level <= 0:
                            print("Leaf nodes affected, please rerun full NEMO")
                            return set(), resource_limit, 0

                        # no convergence, recaclulate from this level up to top
                        # print("Converged with", new_parents, "->", downstream_node, ", recalculating from level", level)
                        all_chs = self.df_nemo[self.df_nemo["level"] == level].index.to_list()
                        current_num_clusters = len(self.opt_dict_levels[level].keys())
                        upstream_nodes_dict = self.merge_clusters_kmeans(all_chs, current_num_clusters, 1)
                        opt_dict_levels, resource_limit = self.nemo_placement(upstream_nodes_dict, 0, level=level)
                        self.opt_dict_levels.update(opt_dict_levels)
                        new_chs = self.df_nemo[self.df_nemo["level"] >= level].index.to_list()
                        new_cluster_heads.update(new_chs)

                        if tree_reopt_level is np.nan:
                            tree_reopt_level = level
                        break

                    if opt is None:
                        resource_limit = True

                    new_cluster_heads.update(new_parents)
                    self.df_nemo.loc[list(new_parents), "level"] = level
                    upstream_nodes = list(new_parents)
                    level += 1

        return new_cluster_heads, resource_limit, tree_reopt_level

    def merge_clusters_kmeans(self, cluster_heads, current_num_clusters, merge_factor):
        idxs = list(set(cluster_heads))
        coords = self.df_nemo.loc[idxs, ["x", "y"]]
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
            s1 = self.df_nemo.iloc[upstream_nodes][["x", "y"]].mean()
            s2 = self.df_nemo.iloc[downstream_node][["x", "y"]].mean()
            w = self.step_size
            # w = len(upstream_nodes) / len(downstream_node)
            opt = util.calc_opt(s1, s2, w=w)

        # preform knn search
        # print("Performing knn for", [opt[0], opt[1]], "with k=", k)
        df_knn = self.df_nemo.iloc[list(self.knn_nodes)]
        full_kdtree = cKDTree(df_knn[["x", "y"]])
        idx_order = full_kdtree.query([opt[0], opt[1]], k=len(self.knn_nodes))[1]
        idx_order = df_knn['oindex'].iloc[idx_order].tolist()

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
            coords = prim_df[["x", "y"]].to_numpy()
            eval_matrix_slots[capacity_col] = evaluate(df_dict[capacity_col], coords)

    if with_eval:
        return eval_matrix_slots, df_dict, opt_dict, limits_dict
    else:
        return df_dict, opt_dict, limits_dict
