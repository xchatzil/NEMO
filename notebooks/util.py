import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.transforms import Bbox

lcl = "black"
cmarker = "p"
ccolor = lcl
coordinator_label = Line2D([], [], color=lcl, marker=cmarker, linestyle='None', label='sink')
worker_label = Line2D([], [], color="grey", marker='o', linestyle='None', label='sources', markersize=4)
reassigned_label = Line2D([0, 1], [0, 1], linestyle='--', color=lcl, label='reassigned')
centroid_label = Line2D([], [], color="grey", marker='o', linestyle='None', label='centroid')
ch_label = Line2D([], [], color=lcl, marker='x', linestyle='None', label='cluster head')
log_opt_label = Line2D([], [], color=lcl, marker='+', linestyle='None', label='optimal cluster head')


def get_max_by_thresh(elements, threshold):
    max_k = np.argmax(elements)
    min_t = max(0, elements[max_k] - threshold)
    print(min_t)
    for i in range(len(elements) - 1, -1, -1):
        if elements[i] >= min_t:
            return i
        i += i
    return max_k


def calc_opt(point1, point2, w1=0.5, w2=0.5, k=0.1, num_iterations=100):
    # Convert input points to numpy arrays
    point1 = np.array(point1, dtype=np.float64)
    point2 = np.array(point2, dtype=np.float64)

    for _ in range(num_iterations):
        # Calculate the displacement vector from point1 to point2
        displacement = point2 - point1

        # Calculate the force vector using Hooke's Law
        force_vector = k * displacement

        # Update the positions of point1 and point2 based on the force
        point1 += np.round(w1 * force_vector, 2)
        point2 -= np.round(w2 * force_vector, 2)

    # The optimal point location is the average of point1 and point2
    optimal_location = (point1 + point2) / 2.0
    return optimal_location


# Function to find the path to the root
def find_path_to_root(df, coords, child_idx, root_idx=0):
    path = []  # Initialize an empty list to store the path

    # Define a recursive function to trace the path
    latency = 0

    def trace_path(index):
        nonlocal latency
        row = df[df['oindex'] == index]
        path.append(index)
        parent_index = row['parent'].values[0]
        # print(index, parent_index)
        latency += np.linalg.norm(row[["x", "y"]].to_numpy() - coords[parent_index])
        if parent_index != root_idx:
            trace_path(parent_index)
        else:
            path.append(root_idx)

    trace_path(child_idx)  # Start tracing the path from the target_index
    return path, latency  # Reverse the path to go from root to target


def evaluate(df, coords):
    device_number = df.shape[0]
    latencies = np.zeros(device_number)
    lookup = {0: 0}

    for i in range(1, device_number):
        idx = df.loc[i, "oindex"]
        if idx in lookup:
            latency = lookup[idx]
        else:
            path, latency = find_path_to_root(df, coords, idx, root_idx=0)
            lookup[idx] = latency
        latencies[i] = latency

    df["latency"] = latencies
    df["load"] = df["total_capacity"] - df["free_capacity"]

    group = df.groupby("oindex")[["latency", "load"]].mean()

    statistics = {"latency_distribution": group["latency"].to_numpy(),
                  "received_packets": group["load"].to_numpy()}
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


def plot(ax, df_origin, df_plcmnt, colors, lval=0.2, leg_size=8, plot_centroids=False, plot_lines=False):
    line_style = "--"
    clusters = df_origin["cluster"].unique()

    for cluster in clusters:
        # plot points
        dfc = df_origin[df_origin["cluster"] == cluster]
        ax.scatter(dfc["x"], dfc["y"], s=10, color=lighten_color(colors[cluster], lval), zorder=-1)

        df_cluster = df_plcmnt[df_plcmnt["cluster"] == cluster]

        parents = df_cluster["parent"].unique()
        if plot_centroids:
            point1 = df_cluster[["x", "y"]].mean()
            ax.scatter(point1["x"], point1["y"], s=50, color=colors[cluster], zorder=10, label="centroid")

            for parent in parents:
                if parent != 0:
                    point2 = df_origin.iloc[parent][["x", "y"]]
                    ax.scatter(point2["x"], point2["y"], s=50, color=colors[cluster], zorder=10, marker="x",
                               label="agg. point")

                    x_values = [point1["x"], point2["x"]]
                    y_values = [point1["y"], point2["y"]]
                    if plot_lines:
                        ax.plot(x_values, y_values, "-", zorder=3, color=colors[cluster])

        for parent in parents:
            # point 1 -> parent
            point1 = df_origin.iloc[parent][["x", "y"]]
            ax.scatter(point1["x"], point1["y"], s=50, color=colors[cluster], zorder=10, marker="x",
                       label="agg. point")

            # point 2 -> parent of parent
            parent_parents = df_plcmnt[df_plcmnt["oindex"] == parent][["parent"]]["parent"].unique()
            point2 = df_origin.iloc[parent_parents][["x", "y"]]
            ax.scatter(point2["x"], point2["y"], s=50, color=colors[cluster], zorder=10, marker="x",
                       label="agg. point")

            for pp in parent_parents:
                # plot connections
                point2 = df_origin.iloc[pp][["x", "y"]]
                x_values = [point1["x"], point2["x"]]
                y_values = [point1["y"], point2["y"]]
                if plot_lines:
                    ax.plot(x_values, y_values, line_style, zorder=3, color=colors[cluster])

    ax.scatter(df_plcmnt.loc[0, "x"], df_plcmnt.loc[0, "y"], s=100, color=ccolor, marker=cmarker, zorder=10)

    ax.set_xlabel('$network$ $coordinate_1$')
    ax.set_ylabel('$network$ $coordinate_2$')

    if plot_centroids:
        handles = [coordinator_label, worker_label, centroid_label, ch_label]
    else:
        handles = [coordinator_label, worker_label, ch_label]

    ax.legend(handles=handles, loc="upper left",
              bbox_to_anchor=(0, 1), prop={'size': leg_size})


def plot_optimum(ax, df_origin, opt_dicts, colors, lval=0.2, leg_size=8, plot_centroid=False, plot_lines=False):
    ccords = df_origin.loc[0, ["x", "y"]].tolist()
    clusters = df_origin["cluster"][df_origin["cluster"] >= 0].unique()
    for cluster in clusters:
        df_cluster = df_origin[df_origin["cluster"] == cluster]
        ax.scatter(df_cluster["x"], df_cluster["y"], s=10, color=lighten_color(colors[cluster], lval), zorder=-1)

        point2 = opt_dicts[1][cluster]
        ax.scatter(point2[0], point2[1], s=50, color=colors[cluster], zorder=10, marker="+",
                   label="agg. point")

        if plot_centroid:
            point1 = df_cluster[["x", "y"]].mean().tolist()
            ax.scatter(point1[0], point1[1], s=50, color=colors[cluster], zorder=10, label="centroid")
            if plot_lines:
                ax.plot([point1[0], point2[0]], [point1[1], point2[1]], "--", zorder=3, color=colors[cluster])
                ax.plot([point2[0], ccords[0]], [point2[1], ccords[1]], "--", zorder=3, color=colors[cluster])

    ax.scatter(df_origin.loc[0, "x"], df_origin.loc[0, "y"], s=100, color=ccolor, marker=cmarker, zorder=10)

    ax.set_xlabel('$network$ $coordinate_1$')
    ax.set_ylabel('$network$ $coordinate_2$')

    if plot_centroid:
        handles = [coordinator_label, worker_label, centroid_label, log_opt_label]
    else:
        handles = [coordinator_label, worker_label, log_opt_label]
    ax.legend(handles=handles, loc="upper left",
              bbox_to_anchor=(0, 1), prop={'size': leg_size})


def plot_topology(ax, df, colors=None, title="Topology"):
    c_coords = df.loc[0, ["x", "y"]].to_numpy()

    if colors is not None:
        labels = df["cluster"].to_numpy()
        df.plot.scatter(ax=ax, x="x", y="y", color=colors[labels], s=df["capacity_" + str(100)] * 0.15)
    else:
        df.plot.scatter(ax=ax, x="x", y="y", c="grey", s=df["capacity_" + str(100)] * 0.15)

    ax.scatter(c_coords[0], c_coords[1], s=100, marker=cmarker, color='black')

    ax.legend(handles=[coordinator_label, worker_label], loc="upper left", bbox_to_anchor=(0, 1), fontsize=8)
    ax.set_xlabel('$network$ $coordinate_1$', fontsize=18)
    ax.set_ylabel('$network$ $coordinate_2$', fontsize=18)
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
