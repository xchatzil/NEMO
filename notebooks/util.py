import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.transforms import Bbox
import math, heapq
from scipy.spatial import cKDTree, voronoi_plot_2d, Voronoi
from scipy.optimize import minimize
from sklearn.metrics import confusion_matrix

lcl = "black"
cmarker = "D"
ccolor = lcl
lnode_marker = "^"
ch_marker = "x"

coordinator_label = Line2D([], [], color=lcl, marker=cmarker, linestyle='None', label='sink')
worker_label = Line2D([], [], color="grey", marker='o', linestyle='None', label='sources', markersize=4)
reassigned_label = Line2D([0, 1], [0, 1], linestyle='--', color=lcl, label='reassigned')
centroid_label = Line2D([], [], color="grey", marker='o', linestyle='None', label='centroid')
ch_label = Line2D([], [], color=lcl, marker=ch_marker, linestyle='None', label='physical node')
log_opt_label = Line2D([], [], color=lcl, marker=lnode_marker, linestyle='None', label='virtual node')


def get_max_by_thresh(elements, threshold):
    max_k = np.argmax(elements)
    min_t = max(0, elements[max_k] - threshold)
    for i in range(len(elements) - 1, -1, -1):
        if elements[i] >= min_t:
            return i
        i += i
    return max_k


def calculate_purity(labels_true, labels_pred):
    # Create a confusion matrix
    cm = confusion_matrix(labels_true, labels_pred)

    # Calculate purity
    purity = 0
    for i, cluster in enumerate(cm):
        purity += cluster[i]

    purity /= len(labels_true)

    return cm, purity


def euclidean_distance(point1, point2):
    """
    Calculate the Euclidean distance between two points represented as tuples.
    """
    return np.linalg.norm(point1 - point2)


def update_coordinates(origin_df, new_coords_df):
    # Update DataFrame based on values in another DataFrame
    df = origin_df.merge(new_coords_df, left_on='oindex', right_index=True, suffixes=('_orig', '_update'), how='left')

    # Choose values from the updated columns, fill NaN with the original values
    df['x'] = df['x_update'].fillna(df['x_orig'])
    df['y'] = df['y_update'].fillna(df['y_orig'])

    # Drop the intermediate columns
    df = df.drop(columns=['x_orig', 'y_orig', 'x_update', 'y_update'])
    return df


def get_coords(existing_coords, target_distances):
    def objective_function(coords, *args):
        existing_coords, target_distances = args
        errors = []
        for i in range(len(existing_coords)):
            current_distance = euclidean_distance(coords, existing_coords[i])
            errors.append((current_distance - target_distances[i]) ** 2)
        return sum(errors)

    # Initial guess for the coordinates of the 6th node
    initial_guess = np.zeros(2)

    # Use optimization to find the coordinates of the node
    result = minimize(objective_function, initial_guess, args=(existing_coords, target_distances), method='Powell')
    # coordinates, error
    return result.x, result.fun


def fit_coords(node_id, coord_df, rtt_df, k_neigh):
    n_coords = rtt_df.loc[node_id, ["x", "y"]]
    df = rtt_df.drop(node_id)
    df = df.sample(k_neigh)[["x", "y"]]
    df['latency'] = list(zip(df.x, df.y))
    df['latency'] = df['latency'].apply(lambda x: np.linalg.norm(x - n_coords))

    existing_coords = coord_df.loc[df.index][["x", "y"]].to_numpy()
    target_distances = df["latency"].to_numpy()
    new_coords, error = get_coords(existing_coords, target_distances)
    return new_coords, error


def get_nested_parents(node_ids, df, parent_col="parent"):
    parents = df.loc[node_ids, parent_col].to_list()
    parents = [tup[0] for sublist in parents if sublist for tup in sublist]
    parents = list(set(parents))
    return parents


def create_groups(coordinates, n):
    """
    Create groups of n closest points such that no index is alone in a group.

    Args:
        coordinates (list of tuples): List of tuples representing coordinates.
        n (int): Number of closest points per group.

    Example:
        coordinates = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)], n = 3
        Result: [[0, 1, 2], [3, 4]]

    Returns:
        list of lists: List of groups where each group is a list of indices.
    """
    num_points = len(coordinates)
    groups = []
    visited = set()

    for i in range(num_points):
        if i not in visited:
            group = [i]
            visited.add(i)

            # Create a min-heap for storing (distance, index) pairs
            min_heap = [(euclidean_distance(coordinates[i], coordinates[j]), j) for j in range(num_points) if j != i]

            while len(group) < n:
                if not min_heap:
                    break

                # Get the closest point from the heap
                distance, closest_point = heapq.heappop(min_heap)

                # If the closest point is unvisited, add it to the group
                if closest_point not in visited:
                    group.append(closest_point)
                    visited.add(closest_point)

            groups.append(group)
    return groups


def calc_opt(point1, point2, w=0.5, k=0.1, num_iterations=100):
    # Convert input points to numpy arrays
    point1 = np.array(point1, dtype=np.float64)
    point2 = np.array(point2, dtype=np.float64)
    w2 = 1 - w

    for _ in range(num_iterations):
        # Calculate the displacement vector from point1 to point2
        displacement = point2 - point1

        # Calculate the force vector using Hooke's Law
        force_vector = k * displacement

        # Update the positions of point1 and point2 based on the force
        point1 += np.round(w * force_vector, 2)
        point2 -= np.round(w2 * force_vector, 2)

    # The optimal point location is the average of point1 and point2
    optimal_location = (point1 + point2) / 2.0
    return optimal_location


def evaluate_node_rec(node_id, df_placement, coords, df_rtt=None):
    node_df = df_placement[df_placement["oindex"] == node_id][["x", "y", "parent"]]
    latencies = []

    for idx, row in node_df.iterrows():
        parent_idx = row["parent"]

        if pd.isna(parent_idx):
            return [0]
        else:
            parent_lats = evaluate_node_rec(parent_idx, df_placement, coords)

            # latency is distance to parent + latency of parent
            if df_rtt is not None:
                latency = df_rtt.loc[idx, parent_idx]
            else:
                self_coords = row[["x", "y"]].to_numpy()
                parent_coords = coords.loc[parent_idx, ["x", "y"]].to_numpy()
                latency = np.linalg.norm(self_coords - parent_coords)

            latency += np.mean(parent_lats)
            latencies.append(latency)
    return latencies


def evaluate(df_placement, df_rtt=None):
    latency_dict = {}

    coords = df_placement.groupby('oindex')[["x", "y"]].first()
    df_sorted = df_placement.sort_values(by='level', ascending=False)

    calculated_parents = set()

    for level, level_df in df_sorted.groupby('level', sort=False):
        for idx, row in level_df.iterrows():
            self_idx = row["oindex"]
            parent_idx = row["parent"]

            if pd.isna(parent_idx):
                latency_dict[self_idx] = [0]
                continue
            if self_idx in calculated_parents:
                continue

            self_coords = row[["x", "y"]].to_numpy()
            parent_coords = coords.loc[parent_idx, ["x", "y"]].to_numpy()

            # latency is distance to parent + latency of parent
            if df_rtt is not None:
                latency = df_rtt.loc[self_idx, parent_idx]
            else:
                latency = np.linalg.norm(self_coords - parent_coords)

            if parent_idx not in latency_dict:
                parent_lats = evaluate_node_rec(parent_idx, df_placement, coords, df_rtt)
                latency_dict[parent_idx] = parent_lats
                calculated_parents.add(parent_idx)

            latency += np.mean(latency_dict[parent_idx])

            if self_idx in latency_dict:
                latency_dict[self_idx] += [latency]
            else:
                latency_dict[self_idx] = [latency]

    df_placement["load"] = df_placement["total_capacity"] - df_placement["free_capacity"]
    load_dict = df_placement.groupby("oindex")["load"].mean().to_dict()
    latency_dict = {key: np.mean(values) for key, values in latency_dict.items()}

    statistics = {"latency_distribution": latency_dict,
                  "received_packets": load_dict}
    df_stats = pd.DataFrame(statistics)
    return df_stats


def replace(list, old_elem, new_elem):
    for pos, val in enumerate(list):
        if val == old_elem:
            list[pos] = new_elem


def lognorm_params(mode, stddev):
    """
    Given the mode and std. dev. of the log-normal distribution, this function
    returns the shape and scale parameters for scipy's parameterization of the
    distribution.
    """
    p = np.poly1d([1, -1, 0, 0, -(stddev / mode) ** 2])
    r = p.roots
    sol = r[(r.imag == 0) & (r.real > 0)].real
    shape = np.sqrt(np.log(sol))
    scale = mode * sol
    return shape, scale


def lighten_color(color, amount=0.5):
    """
    Lightens the given color by multiplying (1-luminosity) by the given amount.
    Input can be matplotlib color string, hex string, or RGB tuple.

    Examples:
    >> lighten_color('g', 0.3)
    >> lighten_color('#F034A3', 0.6)
    >> lighten_color((.3,.55,.1), 0.5)
    """
    import matplotlib.colors as mc
    import colorsys
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


def get_diff(arr1, arr2):
    output = list(set(arr1).symmetric_difference(set(arr2)))
    intersection = list(set(arr1).intersection(arr2))

    for ele in intersection:
        if not np.array_equiv(arr1[ele], arr2[ele]):
            output.append(ele)
    return output


def get_color_list(num_colors):
    colors = sns.color_palette(n_colors=num_colors)
    # colors.insert(0, "black")
    colors_hex = np.asarray(colors.as_hex())
    light_colors = [lighten_color(x) for x in colors_hex]
    return colors_hex, light_colors, colors


def plot_with_single_color(*args, **kwargs):
    args = list(args)
    length = args[1]["cluster"].nunique()
    color = args[3]
    colors = np.full(length, color)
    args[3] = colors
    args = tuple(args)
    plot(*args, **kwargs)


def plot(ax, df_origin, df_plcmnt, colors, lval=0.2, symbol_size=100, scale_fac=0.25, leg_size=12, axis_label_size=20,
         line_style="-", line_width=0.8, highlight_color=None, plot_centroids=False, plot_lines=False, opt_dict=None):
    handles = [coordinator_label, worker_label, ch_label]
    clusters = df_origin["cluster"].unique()
    levels = df_plcmnt.loc[0, "level"]
    df_plcmnt = df_plcmnt.dropna()

    for cluster in clusters:
        # plot points
        dfc = df_origin[df_origin["cluster"] == cluster]
        ax.scatter(dfc["x"], dfc["y"], s=scale_fac * symbol_size, color=lighten_color(colors[cluster], lval), zorder=-1)

        df_cluster = df_plcmnt[df_plcmnt["cluster"] == cluster]

        parents = df_cluster["parent"].unique()
        if plot_centroids:
            point1 = df_cluster[["x", "y"]].mean()
            ax.scatter(point1["x"], point1["y"], s=symbol_size, color=colors[cluster], zorder=4, label="centroid")

            for parent in parents:
                if parent != 0:
                    point2 = df_origin.iloc[parent][["x", "y"]]
                    level = df_plcmnt[df_plcmnt["oindex"] == parent]["level"].to_numpy()[0]

                    child = df_plcmnt[df_plcmnt["parent"] == parent].iloc[0]["oindex"]
                    is_leaf = df_plcmnt[df_plcmnt["parent"] == child].empty

                    x_values = [point1["x"], point2["x"]]
                    y_values = [point1["y"], point2["y"]]
                    if plot_lines and (level == 1 or is_leaf):
                        ax.plot(x_values, y_values, "-", linewidth=line_width, zorder=3, color=colors[cluster])

        for parent in parents:
            # point 1 -> parent
            point1 = df_origin.iloc[parent][["x", "y"]]
            level = df_plcmnt[df_plcmnt["oindex"] == parent]["level"].max()
            if highlight_color is not None and level == levels - 1:
                color = highlight_color
                zorder = 5
            else:
                color = colors[cluster]
                zorder = 4
            ax.scatter(point1["x"], point1["y"], s=symbol_size, color=color, zorder=zorder, marker=ch_marker,
                       label="agg. point")

            # point 2 -> parent of parent
            parent_parents = df_plcmnt[df_plcmnt["oindex"] == parent][["parent"]]["parent"].unique()
            point2 = df_origin.iloc[parent_parents][["x", "y"]]
            if highlight_color is not None and level == levels - 2:
                color = highlight_color
                zorder = 4
            else:
                color = colors[cluster]
                zorder = 5

            ax.scatter(point2["x"], point2["y"], s=symbol_size, color=color, zorder=zorder, marker=ch_marker,
                       label="agg. point")

            for pp in parent_parents:
                # plot connections
                point2 = df_origin.iloc[pp][["x", "y"]]
                x_values = [point1["x"], point2["x"]]
                y_values = [point1["y"], point2["y"]]
                if plot_lines:
                    ax.plot(x_values, y_values, line_style, linewidth=line_width, zorder=3, color=colors[cluster])

    if opt_dict is not None:
        cl_dict = opt_dict[len(opt_dict)]
        for cl_label, opt_coords in cl_dict.items():
            if highlight_color is not None:
                color = highlight_color
            else:
                color = colors[cl_label]

            ax.scatter(opt_coords[0], opt_coords[1], s=symbol_size, color=color, zorder=6, marker=lnode_marker,
                       label="agg. point")

    ax.scatter(df_origin.loc[0, "x"], df_origin.loc[0, "y"], s=2 * symbol_size, color=ccolor, marker=cmarker, zorder=20)

    ax.set_xlabel('$network$ $coordinate_1$', fontsize=axis_label_size)
    ax.set_ylabel('$network$ $coordinate_2$', fontsize=axis_label_size)

    if plot_centroids:
        handles.append(centroid_label)

    if opt_dict is not None:
        handles.append(log_opt_label)

    ax.legend(handles=handles, loc="upper left",
              bbox_to_anchor=(0, 1), prop={'size': leg_size})


def plot_optimum(ax, df_origin, opt_dicts, colors, lval=0.2, symbol_size=100, scale_fac=0.25, leg_size=12,
                 axis_label_size=20, plot_centroid=False, plot_lines=False):
    ccords = df_origin.loc[0, ["x", "y"]].tolist()
    clusters = df_origin["cluster"][df_origin["cluster"] >= 0].unique()
    for cluster in clusters:
        df_cluster = df_origin[df_origin["cluster"] == cluster]
        ax.scatter(df_cluster["x"], df_cluster["y"], s=scale_fac * symbol_size,
                   color=lighten_color(colors[cluster], lval),
                   zorder=-1)

        point2 = opt_dicts[1][cluster]
        ax.scatter(point2[0], point2[1], s=symbol_size, color=colors[cluster], zorder=4, marker=lnode_marker,
                   label="agg. point")

        if plot_centroid:
            point1 = df_cluster[["x", "y"]].mean().tolist()
            ax.scatter(point1[0], point1[1], s=symbol_size, color=colors[cluster], zorder=10, label="centroid")
            if plot_lines:
                ax.plot([point1[0], point2[0]], [point1[1], point2[1]], "--", zorder=3, color=colors[cluster])
                ax.plot([point2[0], ccords[0]], [point2[1], ccords[1]], "--", zorder=3, color=colors[cluster])

    ax.scatter(df_origin.loc[0, "x"], df_origin.loc[0, "y"], s=2 * symbol_size, color=ccolor, marker=cmarker, zorder=5)

    ax.set_xlabel('$network$ $coordinate_1$', fontsize=axis_label_size)
    ax.set_ylabel('$network$ $coordinate_2$', fontsize=axis_label_size)

    if plot_centroid:
        handles = [coordinator_label, worker_label, centroid_label, log_opt_label]
    else:
        handles = [coordinator_label, worker_label, log_opt_label]
    ax.legend(handles=handles, loc="upper left",
              bbox_to_anchor=(0, 1), prop={'size': leg_size})


def plot_topology(ax, df, colors=None, plot_voronoi=False, plot_centroid=False, centroids=None, title="Topology", symbol_size=100,
                  lval=0.2, scale_fac=0.25, centroid_color="grey", point_color="grey", leg_size=12, axis_label_size=20):
    c_coords = df.loc[0, ["x", "y"]].tolist()
    clusters = df["cluster"][df["cluster"] >= 0].unique()

    for cluster in clusters:
        df_cluster = df[df["cluster"] == cluster]

        if colors is not None:
            ax.scatter(df_cluster["x"], df_cluster["y"], s=scale_fac * symbol_size,
                       color=lighten_color(colors[cluster], lval), zorder=-1)
            centroid_color = colors[cluster]
        else:
            ax.scatter(df_cluster["x"], df_cluster["y"], s=scale_fac * symbol_size,
                       color=lighten_color(point_color, lval), zorder=-1)

        if plot_centroid:
            if centroids is None:
                centroid = df_cluster[["x", "y"]].mean()
            else:
                centroid = centroids[cluster]
            ax.scatter(centroid["x"], centroid["y"], s=symbol_size, color=centroid_color, zorder=3,
                       label="centroid")

    if plot_voronoi:
        if centroids is None:
            centroids = df[df["cluster"] >= 0].groupby("cluster")[["x", "y"]].mean().to_numpy()
        vor = Voronoi(centroids)
        voronoi_plot_2d(vor, ax=ax, point_size=symbol_size, color="red", show_vertices=False, show_points=False)

    ax.scatter(c_coords[0], c_coords[1], s=2 * symbol_size, marker=cmarker, color='black')

    if plot_centroid:
        handles = [coordinator_label, worker_label, centroid_label]
    else:
        handles = [coordinator_label, worker_label]

    ax.legend(handles=handles, loc="upper left", bbox_to_anchor=(0, 1), fontsize=leg_size)
    ax.set_xlabel('$network$ $coordinate_1$', fontsize=axis_label_size)
    ax.set_ylabel('$network$ $coordinate_2$', fontsize=axis_label_size)
    ax.set_title(title)
    return ax


def plot_groups(ax, df, colors):
    c_coords = df.loc[0, ["x", "y"]].to_numpy()
    labels = df["cluster"].to_numpy()
    df.plot.scatter(ax=ax, x="x", y="y", color=colors[labels], s=df["capacity_" + str(100)] * 0.15)

    ax.scatter(c_coords[0], c_coords[1], s=100, marker=cmarker, color='black')

    ax.legend(handles=[coordinator_label, worker_label], loc="upper left", bbox_to_anchor=(0, 1), fontsize=8)
    ax.set_xlabel('$network$ $coordinate_1$')
    ax.set_ylabel('$network$ $coordinate_2$')
    return ax


def full_extent(ax, pad=0.0):
    """Get the full extent of an axes, including axes labels, tick labels, and
    titles."""
    # For text objects, we need to draw the figure first, otherwise the extents
    # are undefined.
    ax.figure.canvas.draw()
    items = ax.get_xticklabels() + ax.get_yticklabels()
    items += [ax, ax.title, ax.xaxis.label, ax.yaxis.label]
    items += [ax, ax.title]
    bbox = Bbox.union([item.get_window_extent() for item in items])

    return bbox.expanded(1.0 + pad, 1.0 + pad)
