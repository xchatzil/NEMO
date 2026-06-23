import networkx as nx
import itertools as it
from util import coordinator_label, worker_label, cmarker, lighten_color

import numpy as np


class Operator:
    def __init__(self, operator_id, node_id, rep_factor, rep_id=0):
        self.operator_id = operator_id
        self.node_id = node_id
        self.rep_factor = rep_factor
        self.rep_id = rep_id

    def __eq__(self, other):
        return (self.operator_id == other.operator_id) and (
                self.rep_id == other.rep_id)

    def __str__(self):
        return str(self.operator_id) + "/" + str(self.rep_id)

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash((self.operator_id, self.node_id, self.rep_factor, self.rep_id))


class Link:
    def __init__(self, start_op, end_op):
        self.start_op = start_op
        self.end_op = end_op

    def __str__(self):
        return str(self.start_op) + "->" + str(self.end_op)

    def __repr__(self):
        return self.__str__()


class OperatorPlan:
    def __init__(self, ops, src, sink, links):
        self.ops = ops
        self.sources = src
        self.sinks = sink
        self.links = links

    def get_unpinned_ops(self):
        out = []
        for o in self.ops:
            if (o not in self.sources) and (o not in self.sinks):
                out.append(o)
        return out

    def get_in_links(self, op):
        out = []
        for link in self.links:
            if link.start_op == op:
                out.append(link)
        return out

    def get_out_links(self, op):
        out = []
        for link in self.links:
            if link.end_op == op:
                out.append(link)
        return out

    def replicate(self):
        for n in self.get_unpinned_ops():
            if n.rep_factor > 1:
                in_links = self.get_in_links(n)
                out_links = self.get_out_links(n)
                for i in range(0, n.rep_factor):
                    rep_node = Operator(n.operator_id, n.node_id, n.rep_factor, i + 1)
                    self.ops.append(rep_node)
                    for in_link in in_links:
                        self.links.append(Link(rep_node, in_link.end_op))
                    for out_link in out_links:
                        self.links.append(Link(out_link.start_op, rep_node))
                self.ops.remove(n)
                for l in in_links:
                    self.links.remove(l)
                for l in out_links:
                    self.links.remove(l)

    def create_nx_graph(self):
        G = nx.DiGraph()
        for l in self.links:
            G.add_edge(l.start_op, l.end_op)
        return G


class OptimalPlacer:
    def __init__(self, ncs, operator_plan):
        self.ncs = ncs
        self.operator_plan = operator_plan
        self.nx_graph = self.operator_plan.create_nx_graph()

    def get_mappings_iter(self):
        nodes = self.ncs.index.tolist()
        unpinned_ops = self.operator_plan.get_unpinned_ops()
        all_opts = []

        for u in unpinned_ops:
            tmp = []
            for r in it.product([u], nodes):
                tmp.append((r[0], r[1]))
            all_opts.append(tmp)
        return it.product(*all_opts)

    def get_mappings_iter_rep(self):
        nodes = self.ncs.index.tolist()
        unpinned_ops = self.operator_plan.get_unpinned_ops()
        all_opts = []

        for u in unpinned_ops:
            rep_opts = []
            for v in range(0, u.rep_factor):
                rep_u = Operator(u.operator_id, u.node_id, u.rep_factor, v + 1)
                tmp = []
                for r in it.product([rep_u], nodes):
                    tmp.append((r[0], r[1]))  # operator -> node
                rep_opts.append(tmp)
            if u.rep_factor > 1:
                all_opts.append(list(it.product(*rep_opts)))
            else:
                all_opts.append(rep_opts[0])
        return it.product(*all_opts)

    def cost_for(self, x, y):
        a = self.ncs.loc[x, ["x", "y"]].to_numpy()
        b = self.ncs.loc[y, ["x", "y"]].to_numpy()
        dist = np.linalg.norm(b - a)
        return dist

    def get_used_capacity(self, operators, graph):
        capacity = 0
        for link in graph.edges:
            for o in operators:
                if (link[1] == o) and (link[0] not in operators):
                    capacity = capacity + 1
        return capacity

    def is_valid(self, mapping, capacity_col="slots_100"):
        node_op_dict = mapping_to_node_dict(mapping)
        for node, ops in node_op_dict.items():
            # check if is incoming
            max_capacity = self.ncs.loc[node, [capacity_col]].to_numpy()[0]
            used_capacity = self.get_used_capacity(ops) + len(ops)
            print("Used capacity for " + str(node) + ": " + str(used_capacity) + " with placement " + str(mapping))
            if used_capacity > max_capacity:
                return False
        return True

    def get_valid_paths(self, mapping, capacity_col="slots_100"):
        valid_paths = []
        options = []
        for src in self.operator_plan.sources:
            for sink in self.operator_plan.sinks:
                paths = nx.all_simple_paths(self.nx_graph, src, sink)
                options.append(list(paths))
        possible_paths = it.product(*options)

        node_op_dict = mapping_to_node_dict(mapping)
        for possible_path in possible_paths:
            sub_graph = nx.DiGraph()
            for node_path in possible_path:
                for i in range(0, len(node_path) - 1):
                    sub_graph.add_edge(node_path[i], node_path[i + 1])

            # check if the possible node paths of the current possible path is valid
            valid = True
            for node, ops in node_op_dict.items():
                max_capacity = self.ncs.loc[node, [capacity_col]].to_numpy()[0]
                used_capacity = self.get_used_capacity(ops, sub_graph) + len(ops)
                # print("Used capacity for " + str(node) + ": " + str(used_capacity) + " with placement " + str(mapping))
                if used_capacity > max_capacity:
                    valid = False
            if valid:
                valid_paths.append(possible_path)
        return valid_paths

    def calc_cost(self, mapping):
        # cost is the sum of latency for each source to reach the sink
        cost = 0
        thrown = True

        for src in self.operator_plan.sources:
            for sink in self.operator_plan.sinks:
                # path is a list from src to sink, e.g., [4, 3, 2, 1]
                # print("Path for src " + str(src) + "->" + str(sink))
                # print("Nodes in g: " + str(list(self.nx_graph.nodes)))
                paths = nx.all_simple_paths(self.nx_graph, src, sink)
                for path in paths:
                    for i in range(0, len(path) - 1):
                        x = path[i]
                        y = path[i + 1]
                        x_node = get_node_id_for_op(x, mapping)
                        y_node = get_node_id_for_op(y, mapping)
                        link_cost = self.cost_for(x_node, y_node)
                        # print("Link cost for " + str(x_node) + "->" + str(y_node) + "=" + str(link_cost))
                        cost = cost + link_cost
                        thrown = False
                if thrown:
                    raise Exception("No mapping for " + str(mapping) + " found in paths " + str(list(paths)))
        return cost

    def calc_cost_for_paths(self, paths, mapping):
        # cost is the sum of latency for each source to reach the sink
        cost = 0
        for path in paths:
            for i in range(0, len(path) - 1):
                x = path[i]
                y = path[i + 1]
                x_node = get_node_id_for_op(x, mapping)
                y_node = get_node_id_for_op(y, mapping)
                link_cost = self.cost_for(x_node, y_node)
                # print("Link cost for " + str(x_node) + "->" + str(y_node) + "=" + str(link_cost))
                cost = cost + link_cost
        return cost

    def get_best_mapping(self, with_constraints=True):
        mappings = self.get_mappings_iter()
        # print("Mappings: " + str(list(mappings)))
        best_cost_mapping = float('inf')
        best_mapping = -1
        best_paths = []

        for map_opt in mappings:
            valid = False
            valid_paths = []
            if with_constraints:
                # valid = self.is_valid(map_opt)
                valid_paths = self.get_valid_paths(map_opt)

            if valid or (len(valid_paths) >= 1):
                for paths in valid_paths:
                    current_cost = self.calc_cost_for_paths(paths, map_opt)
                    if current_cost < best_cost_mapping:
                        best_cost_mapping = current_cost
                        best_mapping = map_opt
                        best_paths = paths

        if best_mapping == -1:
            raise Exception("No Mapping Available!")

        return best_mapping, best_paths, best_cost_mapping


def create_window_merging_example(nodes, rep=2):
    ops = []
    links = []
    srcs = []
    sinks = []

    sink = Operator(1, 0, 1)
    sinks.append(sink)
    ops.append(sink)

    final_window = Operator(2, -1, 1)
    slice_merging = Operator(3, -1, rep)
    ops.append(final_window)
    ops.append(slice_merging)

    links.append(Link(final_window, sink))
    links.append(Link(slice_merging, final_window))

    cnt = 3
    for i in range(1, nodes):
        oid = cnt + i
        tmp = Operator(oid, i, 1)
        ops.append(tmp)
        srcs.append(tmp)
        links.append(Link(tmp, slice_merging))

    return OperatorPlan(ops, srcs, sinks, links)


def get_node_id_for_op(x, mapping):
    if x.node_id >= 0:
        return x.node_id
    else:
        # find mapping of op id in mapping
        for m in mapping:
            if m[0] == x:
                return m[1]
    raise Exception("Operator ID " + str(x.operator_id) + " not found in mapping " + str(mapping))


def mapping_to_node_dict(mapping):
    out = {}
    for op, node in mapping:
        if node not in out:
            out[node] = [op]
        else:
            out[node].append(op)
    return out


def mapping_to_op_dict(mapping):
    out = {}
    for op, node in mapping:
        if op not in out:
            out[op] = node
        else:
            raise Exception("Multiple nodes assigned to operator " + str(op))
    return out


def replicate(nx_graph):
    for n in list(nx_graph.nodes):
        if (n.rep_factor > 1):
            in_links = nx_graph.in_edges(n)
            out_links = nx_graph.out_edges(n)
            for i in range(0, n.rep_factor):
                rep_node = Operator(n.operator_id, n.node_id, n.rep_factor, i + 1)
                for in_link in in_links:
                    nx_graph.add_edge(in_link[0], rep_node)
                for out_link in out_links:
                    nx_graph.add_edge(rep_node, out_link[1])
            nx_graph.remove_node(n)
    return nx_graph


def plot(ax, ncs, paths, mapping, lval=0.2):
    tcolors = {"coordinator": "red", "worker": "grey"}
    line_style = "--"

    # plot workers
    ncs.plot.scatter(ax=ax, x="x", y="y", c=ncs["type"].map(tcolors), s=ncs["slots_" + str(100)] * 2.5)

    # plot coordinators
    ccoord = ncs.loc[0, ["x", "y"]].to_numpy()
    ax.scatter(ccoord[0], ccoord[1], s=100, marker=cmarker, color='black')

    # plot links
    for path in paths:
        for i in range(0, len(path) - 1):
            x = get_node_id_for_op(path[i], mapping)
            y = get_node_id_for_op(path[i + 1], mapping)
            point1 = ncs.loc[x, ["x", "y"]].to_numpy()
            point2 = ncs.loc[y, ["x", "y"]].to_numpy()
            x_values = [point1[0], point2[0]]
            y_values = [point1[1], point2[1]]
            ax.plot(x_values, y_values, line_style, zorder=-1, color=lighten_color("black", lval + 0.2))

    ax.legend(handles=[coordinator_label, worker_label], loc="upper left", bbox_to_anchor=(0, 1), fontsize=8)
    ax.set_xlabel('$network$ $coordinate_1$')
    ax.set_ylabel('$network$ $coordinate_2$')
    return ax
